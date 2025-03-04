
class FlowDirection:
    UP = 'UP'
    DOWN = 'DOWN'

class MarketObjectStatus:
    AVAILABLE = 'AVAILABLE'
    UNAVAILABLE = 'UNAVAILABLE'

class BusinessType:
    OFFER = 'OFFER'

# Mapping of XML values to Python objects
XML_FLOW_DIRECTION_MAP = {
    'A01': FlowDirection.UP,
    'A02': FlowDirection.DOWN,
}

XML_MARKET_OBJECT_STATUS_MAP = {
    'A06': MarketObjectStatus.AVAILABLE,
    'A11': MarketObjectStatus.UNAVAILABLE,
}

XML_BUSINESS_TYPE = {
    "B74": BusinessType.OFFER
}