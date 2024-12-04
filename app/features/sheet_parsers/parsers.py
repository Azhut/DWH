from app.features.sheet_parsers.base_sheet_parser import BaseSheetParser
from app.features.sheet_parsers.sheet0_parser import Sheet0Parser
from app.features.sheet_parsers.sheet1_parser import Sheet1Parser
from app.features.sheet_parsers.sheet2_parser import Sheet2Parser
from app.features.sheet_parsers.sheet3_parser import Sheet3Parser
from app.features.sheet_parsers.sheet4_parser import Sheet4Parser
from app.features.sheet_parsers.sheet5_parser import Sheet5Parser
from app.features.sheet_parsers.sheet6_parser import Sheet6Parser
from app.features.sheet_parsers.sheet7_parser import Sheet7Parser


# Регистрация парсеров
PARSERS = {
    "Раздел0": Sheet0Parser,
    "Раздел1": Sheet1Parser,
    "Раздел2": Sheet2Parser,
    "Раздел3": Sheet3Parser,
    "Раздел4": Sheet4Parser,
    "Раздел5": Sheet5Parser,
    "Раздел6": Sheet6Parser,
    "Раздел7": Sheet7Parser,
}


def get_sheet_parser(sheet_name: str) -> BaseSheetParser:
    """
    Возвращает парсер для указанного листа.
    """
    if sheet_name in PARSERS:
        return PARSERS[sheet_name]()
    else:
        raise Exception(f"Неизвестный лист: {sheet_name}")
