from src.intraday.delivery_areas import DeliveryArea
from src.model.cmol.cmol_types import XML_FLOW_DIRECTION_MAP, XML_BUSINESS_TYPE, XML_MARKET_OBJECT_STATUS_MAP

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
        return sum([point.price * point.quantity for point in self.points]) / self.quantity

class CMOL:
    def __init__(self, creation_utc, start_utc, end_utc, bids: [CMOLBid]):
        self.creation_utc = creation_utc
        self.start_utc = start_utc
        self.end_utc = end_utc
        self.bids = bids

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
            bids.append(CMOLBid.from_xml(ts))

        return CMOL(creation_utc=createdDateTime, start_utc=start_time, end_utc=end_time, bids=bids)