
class FlowDirection:
    UP = 'UP'
    DOWN = 'DOWN'

XML_FLOW_DIRECTION_MAP = {
    'A01': FlowDirection.UP,
    'A02': FlowDirection.DOWN,
}

class MarketObjectStatus:
    AVAILABLE = 'AVAILABLE'
    UNAVAILABLE = 'UNAVAILABLE'

XML_MARKET_OBJECT_STATUS_MAP = {
    'A06': MarketObjectStatus.AVAILABLE,
    'A11': MarketObjectStatus.UNAVAILABLE,
}

class BusinessType:
    OFFER = 'OFFER'

XML_BUSINESS_TYPE = {
    "B74": BusinessType.OFFER
}