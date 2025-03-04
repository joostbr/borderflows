from enum import Enum


class ProductTypes:
    P15MIN = "P15MIN"
    P60MIN = "P60MIN"
    P30MIN = "P30MIN"

class DeliveryArea(Enum):
    DK1 = (1,  "10YDK-1--------W", "EUR", "DK1", "DK", [ProductTypes.P15MIN, ProductTypes.P60MIN])
    DK2 = (2,  "10YDK-2--------M", "EUR", "DK2", "DK", [ProductTypes.P15MIN, ProductTypes.P60MIN])
    FI  = (3,  "10YFI-1--------U", "EUR", "FI", "FI", [ProductTypes.P15MIN, ProductTypes.P60MIN])
    FRE = (4,  "10YDOM-1001A084H", "EUR", "FI", "FI", [ProductTypes.P60MIN])
    EE  = (5,  "10Y1001A1001A39I", "EUR", "EE", "EE", [ProductTypes.P60MIN])
    LV  = (6,  "10YLV-1001A00074", "EUR", "LV", "LV", [ProductTypes.P60MIN])
    LT  = (7,  "10YLT-1001A0008Q", "EUR", "LT", "LT", [ProductTypes.P60MIN])
    NO1 = (8,  "10YNO-1--------2", "EUR", "NO1", "NO", [ProductTypes.P15MIN, ProductTypes.P60MIN])
    NO2 = (9,  "10YNO-2--------T", "EUR", "NO2", "NO", [ProductTypes.P15MIN, ProductTypes.P60MIN])
    NO3 = (10, "10YNO-3--------J", "EUR", "NO3", "NO", [ProductTypes.P15MIN, ProductTypes.P60MIN])
    NO4 = (11, "10YNO-4--------9", "EUR", "NO4", "NO", [ProductTypes.P15MIN, ProductTypes.P60MIN])
    NO5 = (12, "10Y1001A1001A48H", "EUR", "NO5", "NO", [ProductTypes.P15MIN, ProductTypes.P60MIN])
    SE1 = (13, "10Y1001A1001A44P", "EUR", "SE1", "SE", [ProductTypes.P15MIN, ProductTypes.P60MIN])
    SE2 = (14, "10Y1001A1001A45N", "EUR", "SE2", "SE", [ProductTypes.P15MIN, ProductTypes.P60MIN])
    SE3 = (15, "10Y1001A1001A46L", "EUR", "SE3", "SE", [ProductTypes.P15MIN, ProductTypes.P60MIN])
    SE4 = (16, "10Y1001A1001A47J", "EUR", "SE4", "SE", [ProductTypes.P15MIN, ProductTypes.P60MIN])

    CNOR = (18, "10Y1001A1001A70O", "EUR", "CNOR", "IT", [])
    COAC = (19, "10Y1001A1001A885", "EUR", "COAC", "IT", [])
    CORS = (20, "10Y1001A1001A893", "EUR", "CORS", "IT", [])
    CSUD = (21, "10Y1001A1001A71M", "EUR", "CSUD", "IT", [])

    MALTA = (23, "10Y1001A1001A877", "EUR", "MALTA", "IT", [])
    NORD = (24, "10Y1001A1001A73I", "EUR", "NORD", "IT", [])

    SARD = (27, "10Y1001A1001A74G", "EUR", "SARD", "IT", [])
    SICI = (28, "10Y1001A1001A75E", "EUR", "SICI", "IT", [])
    SUD = (29, "10Y1001A1001A788", "EUR", "SUD", "IT", [])
    PT  = (30, "10YPT-REN------W", "EUR", "PT", "PT", [ProductTypes.P60MIN])
    ES  = (31, "10YES-REE------0", "EUR", "ES", "ES", [ProductTypes.P60MIN])
    MO  = (32, "10YMA-ONE------O", "EUR", "MO", "MA", [ProductTypes.P60MIN])
    GBP = (33, "10Y1001A1001A57G", "GBP", "UK", "UK", [ProductTypes.P15MIN, ProductTypes.P30MIN, ProductTypes.P60MIN])
    NL  = (34, "10YNL----------L", "EUR", "NL", "NL", [ProductTypes.P15MIN, ProductTypes.P30MIN, ProductTypes.P60MIN])
    BE  = (35, "10YBE----------2", "EUR", "BE", "BE", [ProductTypes.P15MIN, ProductTypes.P30MIN, ProductTypes.P60MIN])
    TBW = (36, "10YDE-ENBW-----N", "EUR", "TBW", "DE", [ProductTypes.P15MIN, ProductTypes.P30MIN, ProductTypes.P60MIN])
    AMP = (37, "10YDE-RWENET---I", "EUR", "AMP", "DE", [ProductTypes.P15MIN, ProductTypes.P30MIN, ProductTypes.P60MIN])
    TTG = (38, "10YDE-EON------1", "EUR", "TTG", "DE", [ProductTypes.P15MIN, ProductTypes.P30MIN, ProductTypes.P60MIN])
    FHZ = (39, "10YDE-VE-------2", "EUR", "50Hz", "DE", [ProductTypes.P15MIN, ProductTypes.P30MIN, ProductTypes.P60MIN])
    FR  = (40, "10YFR-RTE------C", "EUR", "FR", "FR", [ProductTypes.P30MIN, ProductTypes.P60MIN])
    CH  = (41, "10YCH-SWISSGRIDZ", "EUR", "CH", "CH", [])
    AT  = (42, "10YAT-APG------L", "EUR", "AT", "AT", [ProductTypes.P15MIN, ProductTypes.P60MIN])
    DK1A = (43, "10YDK-1-------AA", "EUR", "DK1A", "DK", [ProductTypes.P60MIN])
    NO1A = (44, "10Y1001A1001A64J", "EUR", "NO1A", "NO", [ProductTypes.P60MIN])
    HR  = (45, "10YHR-HEP------M", "EUR", "HR", "HR", [ProductTypes.P60MIN])
    BG = (46, "10YCA-BULGARIA-R", "EUR", "BG", "BG", [ProductTypes.P15MIN, ProductTypes.P60MIN])

    IE = (86, "10Y1001A1001A59C", "EUR", "IE", "IE", [])
    PL = (88, "10YPL-AREA-----S", "EUR", "PL", "PL", [ProductTypes.P60MIN])
    GB2 = (90, "10Y1001A1001A58E", "GBP", "GB2", "UK", [])
    ELE = (92, "38YNPSLVEXP----Y", "EUR", "ELE", "EE", [])
    ELI = (93, "38YNPSLVIMP----5", "EUR", "ELI", "EE", [])
    ERI = (94, "38YNPSRUIMP----S", "EUR", "ERI", "EE", [])
    GER = (95, "10YCB-GERMANY--8", "EUR", "GER", "DE", [])
    LBE = (96, "10Y1001A1001A56I", "EUR", "LBE", "LT", [])
    LBI = (97, "10Y1001A1001A55K", "EUR", "LBI", "LT", [])
    LRE = (98, "43YLRE-------008", "EUR", "LRE", "LV", [])
    LRI = (99, "43YLRI--------04", "EUR", "LRI", "LV", [])
    PLA = (100, "10YDOM-PL-SE-LT2", "EUR", "PLA", "PL", [])
    ELES = (108, "10YSI-ELES-----O", "EUR", "ELES", "SI", [])
    CEPS = (109, "10YCZ-CEPS-----N", "EUR", "CEPS", "CZ", [])
    MAVIR = (110, "10YHU-MAVIR----U", "EUR", "MAVIR", "HU", [])
    TEL = (111, "10YRO-TEL------P", "EUR", "TEL", "RO", [])
    GE = (113, "10Y1001A1001B012", "GEL", "GE", "GE", [ProductTypes.P60MIN])
    GB1A = (114, "11Y0-0000-0265-K", "GBP", "GB1A", "UK", [])
    PLC = (115, "10Y1001C--00038X", "EUR", "PLC", "PL", [])
    ALBE = (116, "22Y201903144---9", "EUR", "ALBE", "BE", [])
    ALDE = (117, "22Y201903145---4", "EUR", "ALDE", "DE", [])
    ITCALA = (118, "10Y1001C--00096J", "EUR", "ITCALA", "IT", [])
    ITFR = (120, "10Y1001A1001A81J", "EUR", "ITFR", "IT", [])
    ITCH = (122, "10Y1001A1001A68B", "EUR", "ITCH", "IT", [])
    ITAT = (124, "10Y1001A1001A80L", "EUR", "ITAT", "IT", [])
    ITSI = (126, "10Y1001A1001A67D", "EUR", "ITSI", "IT", [])
    ITME = (128, "10Y1001C--000611", "EUR", "ITME", "IT", [])
    ITGR = (130, "10Y1001A1001A66F", "EUR", "ITGR", "IT", [])
    ITC = (132, "10YIT-GRTN-----B", "EUR", "ITCP", "IT", [])
    DEA2 = (134, "10YDOM-1001A082L", "EUR", "DEA2", "DE", [])
    SK = (135, "10YSK-SEPS-----K", "EUR", "SK", "SK", [])
    GREE = (136, "10YGR-HTSO-----Y", "EUR", "GREE", "GR", [])
    NO2A = (137, "10Y1001C--001219", "EUR", "NO2A", "NO", [])
    LKAL = (138, "10Y1001A1001A50U", "EUR", "LKAL", "LT", [])
    DE_LU = (139, "10Y1001A1001A82H", "EUR", "DE-LU", "DE", [])
    BY = (140, "10Y1001A1001A51S", "EUR", "BY", "BY", [])
    RU = (141, "10Y1001A1001A49F", "EUR", "RU", "RU", [])
    GB = (142, "10YGB----------A", "GBP", "GB", "UK", [])
    NO2_SK = (143, "50YCUY85S1HH29EK", "EUR", "NO2_SK", "NO", [])
    DK1_SK = (144, "45Y000000000001C", "EUR", "DK1_SK", "DK", [])
    DK1_SB = (145, "45Y0000000000038", "EUR", "DK1_SB", "DK", [])
    DK2_SB = (146, "45Y0000000000062", "EUR", "DK2_SB", "DK", [])
    FI_FS = (147, "44Y-00000000160K", "EUR", "FI_FS", "FI", [])
    SE3_FS = (148, "46Y000000000001Y", "EUR", "SE3_FS", "SE", [])
    DK1_KS = (149, "45Y000000000002A", "EUR", "DK1_KS", "DK", [])
    SE3_KS = (150, "46Y000000000002W", "EUR", "SE3_KS", "SE", [])
    SE4_SP = (151, "46Y000000000003U", "EUR", "SE4_SP", "SE", [])
    SE4_NB = (152, "46Y000000000004S", "EUR", "SE4_NB", "SE", [])
    SE4_BC = (153, "46Y000000000005Q", "EUR", "SE4_BC", "SE", [])
    FI_EL = (154, "44Y-00000000161I", "EUR", "FI_EL", "FI", [])
    DK1_DE = (155, "45Y0000000000054", "EUR", "DK1_DE", "DK", [])
    DK2_KO = (156, "45Y0000000000070", "EUR", "DK2_KO", "DK", [])
    DK1_CO = (157, "45Y0000000000046", "EUR", "DK1_CO", "DK", [])
    NO2_ND = (158, "50Y73EMZ34CQL9AJ", "EUR", "NO2_ND", "NO", [])
    NO2_NK = (159, "50YNBFFTWZRAHA3P", "EUR", "NO2_NK", "NO", [])
    SE3_SWL = (160, "46Y000000000017J", "EUR", "SE3_SWL", "SE", [])
    SE4_SWL = (161, "46Y000000000018H", "EUR", "SE4_SWL", "SE", [])

    def __init__(self, delivery_area_id, eic_code, currency_code, area_code, country_iso_code, product_types):
        self.delivery_area_id = delivery_area_id
        self.eic_code = eic_code
        self.currency_code = currency_code
        self.area_code = area_code
        self.country_iso_code = country_iso_code
        self.product_types = product_types

    @staticmethod
    def get_delivery_area_by_id(delivery_area_id):
        for delivery_area in DeliveryArea:
            if delivery_area.delivery_area_id == delivery_area_id:
                return delivery_area
        return None

    @staticmethod
    def get_delivery_area_by_eic_code(eic_code):
        for delivery_area in DeliveryArea:
            if delivery_area.eic_code == eic_code:
                return delivery_area
        return None

    @staticmethod
    def get_delivery_area_by_area_code(area_code):
        for delivery_area in DeliveryArea:
            if delivery_area.area_code == area_code:
                return delivery_area
        return None


TSO_AREA_MAPPING = {
    "50HZT": DeliveryArea.FHZ,
    "AMP": DeliveryArea.AMP,
    "APG": DeliveryArea.AT,
    "ELIA": DeliveryArea.BE,
    "TNG": DeliveryArea.TBW,
    "TTG": DeliveryArea.TTG,
    "CEPS": DeliveryArea.CEPS,
    "SG": DeliveryArea.CH,
    "TERNA": DeliveryArea.ITC,
    "ESO": DeliveryArea.BG,
    "ENDK1": DeliveryArea.DK1,
    "ENDK2": DeliveryArea.DK2,
    "TNL": DeliveryArea.NL,
    "SEPS": DeliveryArea.SK
}