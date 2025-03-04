from datetime import datetime

import numpy as np
import pandas as pd

from src.intraday.delivery_areas import DeliveryArea
from src.model.cmol.cmol_types import XML_FLOW_DIRECTION_MAP, XML_BUSINESS_TYPE, XML_MARKET_OBJECT_STATUS_MAP, \
    FlowDirection, MarketObjectStatus

# https://eepublicdownloads.entsoe.eu/clean-documents/EDI/Library/Central_Transparency_Platform___IG_for_European_Platforms_v1.0.pdf
# https://energinet.dk/media/12tbl1rn/implementation-guide-afrr-eam-september-2023.pdf

NAMESPACE = {"ns": "urn:iec62325.351:tc57wg16:451-7:moldocument:7:3"}

class CMOLPoint:
    def __init__(self, position, quantity, price):
        self.position = position # according to documentation this thing is always 1
        self.quantity = quantity # 1-9999MW -> 0: CANCELED
        self.price = price

    @staticmethod
    def from_xml(xml):
        parsed = {}
        for child in xml:
            tag_name = child.tag.split("}")[-1]
            parsed[tag_name] = child.text.strip() if child.text else None
        return CMOLPoint(position=parsed["position"], quantity=float(parsed["quantity.quantity"]), price=float(parsed["energy_Price.amount"]))

class CMOLBid:
    def __init__(self, market_agreement_id, acquiring_domain_id, connecting_domain_id, auction_id, business_type, direction, status, points, **kwargs):
        self.market_agreement_id = market_agreement_id
        self.acquiring_domain_id = acquiring_domain_id
        self.connecting_domain_id = connecting_domain_id
        self.auction_id = auction_id
        self.business_type = business_type
        self.direction = direction
        self.status = status
        self.points = points

    @staticmethod
    def from_xml(xml):
        parsed = {}

        # Iterate over direct children of TimeSeries instead of calling `.find()` multiple times
        for child in xml:
            tag_name = child.tag.split("}")[-1]  # Remove namespace prefix
            parsed[tag_name] = child.text.strip() if child.text else None

            # Handle nested Period elements inside TimeSeries
            if tag_name == "Period":
                parsed["resolution"] = child.find(f"{{{NAMESPACE['ns']}}}resolution").text

                # Extract points inside Period
                parsed["points"] = []
                for point in child.iter(f"{{{NAMESPACE['ns']}}}point"):
                    parsed["points"].append(CMOLPoint.from_xml(point))

        return CMOLBid(
            market_agreement_id=parsed["marketAgreement.mRID"],
            acquiring_domain_id=parsed["acquiring_Domain.mRID"],
            connecting_domain_id=DeliveryArea.get_delivery_area_by_eic_code(parsed["connecting_Domain.mRID"]),
            auction_id=parsed["auction.mRID"],
            business_type=XML_BUSINESS_TYPE[parsed["businessType"]],
            direction=XML_FLOW_DIRECTION_MAP[parsed["direction"]],
            status=XML_MARKET_OBJECT_STATUS_MAP[parsed["marketObjectStatus.status"]],
            points=parsed["points"]
        )

    @property
    def quantity(self):
        return sum([point.quantity for point in self.points])

    @property
    def price(self):
        return sum([point.price * point.quantity for point in self.points]) / self.quantity if self.quantity > 0 else None

class CMOL:
    def __init__(self, creation_utc, start_utc, end_utc, bids: [CMOLBid]):
        self.creation_utc = creation_utc
        self.start_utc = start_utc
        self.end_utc = end_utc
        self.bids = bids

    @staticmethod
    def _parse_utc(utc, has_seconds=False):
        return datetime.strptime(utc, '%Y-%m-%dT%H:%M:%SZ' if has_seconds else '%Y-%m-%dT%H:%MZ')

    @staticmethod
    def from_xml(xml):
        # Namespace handling (Extracting the default namespace)


        # Extracting values
        #mRID = xml.find("ns:mRID", namespace).text
        #revisionNumber = xml.find("ns:revisionNumber", namespace).text
        createdDateTime = xml.find("ns:createdDateTime", NAMESPACE).text
        start_time = xml.find("ns:period.timeInterval/ns:start", NAMESPACE).text
        end_time = xml.find("ns:period.timeInterval/ns:end", NAMESPACE).text

        # Output extracted data
        bids = []
        for ts in xml.iter(f"{{{NAMESPACE['ns']}}}TimeSeries"):  # Efficiently iterate over all TimeSeries
            bid = CMOLBid.from_xml(ts)
            if bid.price is not None and bid.quantity > 0:
                bids.append(bid)

        return CMOL(creation_utc=CMOL._parse_utc(createdDateTime, has_seconds=True), start_utc=CMOL._parse_utc(start_time), end_utc=CMOL._parse_utc(end_time), bids=bids)

    def groupby_region(self):
        cmol_by_area_and_direction = {}
        for bid in self.bids:
            key = (bid.connecting_domain_id, bid.direction)
            if key not in cmol_by_area_and_direction:
                cmol_by_area_and_direction[key] = []

            cmol_by_area_and_direction[key].append(bid)

        return cmol_by_area_and_direction

    def transform_bids(self, bids, quantiles, flow_direction):
        result = {}

        bids_sorted = sorted(bids, key=lambda x: x.price if flow_direction == "UP" else -x.price)
        bids_volume = sum([bid.quantity for bid in bids_sorted if bid.status == MarketObjectStatus.AVAILABLE])

        for quantile in quantiles:
            level = quantile * bids_volume

            ## Marginal price
            max_price = None
            quantity = 0
            for bid in bids_sorted:
                if quantity >= max(level, 1): # 1 MW is the minimum quantity
                    break

                if bid.status == MarketObjectStatus.AVAILABLE:
                    max_price = bid.price
                    quantity += bid.quantity

            result[str(int(1000*quantile))] = max_price if quantity > 0 else None

        return result

    def to_lmol_df(self, quantiles):
        cmol_by_area_and_direction = self.groupby_region()

        ups = []
        downs = []
        for (region, direction), bids in cmol_by_area_and_direction.items():
            available_bids = [bid for bid in bids if bid.status == MarketObjectStatus.AVAILABLE]

            transformed = self.transform_bids(available_bids, quantiles=quantiles, flow_direction=direction)
            transformed["REGION"] = region.area_code
            transformed["VOLUME"] = sum([bid.quantity for bid in available_bids])

            if direction == FlowDirection.UP:
                ups.append(transformed)
            else:
                downs.append(transformed)

        ups = pd.DataFrame(ups)
        downs = pd.DataFrame(downs)

        ups = ups.rename(columns={level: f"UP_{level}" for level in ups.columns if level != "REGION"})
        downs = downs.rename(columns={level: f"DOWN_{level}" for level in downs.columns if level != "REGION"})

        df = ups.merge(downs, on="REGION", how="outer")
        df["UTCTIME"] = self.start_utc
        return df

    def to_cmol_df(self, quantiles):
        up_bids = [bid for bid in self.bids if bid.direction == FlowDirection.UP and bid.status == MarketObjectStatus.AVAILABLE]
        down_bids = [bid for bid in self.bids if bid.direction == FlowDirection.DOWN and bid.status == MarketObjectStatus.AVAILABLE]

        up_volume = sum([bid.quantity for bid in up_bids])
        down_volume = sum([bid.quantity for bid in down_bids])

        up_transformed = self.transform_bids(up_bids, quantiles=quantiles, flow_direction=FlowDirection.UP)
        down_transformed = self.transform_bids(down_bids, quantiles=quantiles, flow_direction=FlowDirection.DOWN)

        up_transformed["VOLUME"] = up_volume
        down_transformed["VOLUME"] = down_volume

        concatenated = {f"UP_{k}": v for k,v in up_transformed.items()}
        concatenated.update({f"DOWN_{k}": v for k,v in down_transformed.items()})

        concatenated["UTCTIME"] = self.start_utc
        return pd.DataFrame([concatenated])