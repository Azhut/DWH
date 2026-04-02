"""Обрезка ведущих уровней горизонтальных заголовков (1ФК и опционально requisites)."""

from app.application.parsing.strategies.auto import AutoFormParsingStrategy
from app.application.parsing.strategies.fk1 import FK1FormParsingStrategy
from app.domain.form.models import FormInfo, FormType
from app.domain.parsing.header_parsing import (
    drop_leading_horizontal_path_segments,
    strip_fk1_horizontal_banner_segments,
    strip_horizontal_leading_okei_banner,
    strip_horizontal_leading_section_banner,
)
from app.domain.parsing.vertical_hierarchy_config import PATH_SEPARATOR


def test_drop_zero_is_identity():
    path = f"A{PATH_SEPARATOR}B{PATH_SEPARATOR}C"
    assert drop_leading_horizontal_path_segments(path, 0) == path


def test_drop_two_removes_prefix():
    path = f"Раздел II{PATH_SEPARATOR}ОКЕИ{PATH_SEPARATOR}N строки"
    got = drop_leading_horizontal_path_segments(path, 2)
    assert got == "N строки"


def test_strip_section_only_all_forms():
    path = f"Раздел I{PATH_SEPARATOR}Код по ОКЕИ: 792{PATH_SEPARATOR}N строки"
    assert strip_horizontal_leading_section_banner(path) == (
        f"Код по ОКЕИ: 792{PATH_SEPARATOR}N строки"
    )


def test_strip_section_then_okei_matches_fk1_combined():
    path = (
        f"Раздел II{PATH_SEPARATOR}"
        f"Коды по ОКЕИ: единица - 642{PATH_SEPARATOR}N строки"
    )
    stepwise = strip_horizontal_leading_okei_banner(strip_horizontal_leading_section_banner(path))
    assert stepwise == strip_fk1_horizontal_banner_segments(path) == "N строки"


def test_strip_fk1_removes_section_and_okei_banners():
    path = (
        f"Раздел II. Физкультурно-оздоровительная работа{PATH_SEPARATOR}"
        f"Коды по ОКЕИ: единица - 642{PATH_SEPARATOR}N строки"
    )
    assert strip_fk1_horizontal_banner_segments(path) == "N строки"


def test_strip_fk1_leaves_short_paths_without_section():
    path = f"Единовременная пропускная{PATH_SEPARATOR}из них в сельской местности"
    assert strip_fk1_horizontal_banner_segments(path) == path


def test_strip_fk1_removes_only_okei_when_no_section_word():
    path = f"Код по ОКЕИ: человек - 792{PATH_SEPARATOR}N строки"
    assert strip_fk1_horizontal_banner_segments(path) == "N строки"


def test_drop_never_empties_path_keeps_last_segment():
    path = f"Only{PATH_SEPARATOR}Two"
    assert drop_leading_horizontal_path_segments(path, 2) == "Two"
    assert drop_leading_horizontal_path_segments(path, 99) == "Two"


def test_single_segment_untouched_when_drop_requested():
    assert drop_leading_horizontal_path_segments("N строки", 2) == "N строки"


def test_fk1_strategy_defaults_banner_strip_on():
    strat = FK1FormParsingStrategy()
    info = FormInfo(
        id="i",
        name="1ФК",
        type=FormType.FK_1,
        requisites={"skip_sheets": [0]},
    )
    assert strat.get_horizontal_header_strip_fk1_banner("Раздел1", info) is True


def test_fk1_strategy_requisite_can_disable_banner_strip():
    strat = FK1FormParsingStrategy()
    info = FormInfo(
        id="i",
        name="1ФК",
        type=FormType.FK_1,
        requisites={
            "skip_sheets": [0],
            "horizontal_header_strip_fk1_banner": False,
        },
    )
    assert strat.get_horizontal_header_strip_fk1_banner("Раздел1", info) is False


def test_auto_form_strategy_default_no_drop():
    strat = AutoFormParsingStrategy()
    info = FormInfo(
        id="i",
        name="5ФК",
        type=FormType.FK_5,
        requisites={"skip_sheets": [0]},
    )
    assert strat.get_horizontal_header_leading_levels_to_drop("Раздел1", info) == 0


def test_auto_form_strategy_requisite_enables_drop():
    strat = AutoFormParsingStrategy()
    info = FormInfo(
        id="i",
        name="5ФК",
        type=FormType.FK_5,
        requisites={
            "skip_sheets": [0],
            "horizontal_header_leading_levels_to_drop": 2,
        },
    )
    assert strat.get_horizontal_header_leading_levels_to_drop("Раздел1", info) == 2


def test_auto_form_strategy_fk1_banner_off_by_default():
    strat = AutoFormParsingStrategy()
    info = FormInfo(
        id="i",
        name="5ФК",
        type=FormType.FK_5,
        requisites={"skip_sheets": [0]},
    )
    assert strat.get_horizontal_header_strip_fk1_banner("Раздел1", info) is False


def test_auto_form_strategy_requisite_can_enable_fk1_banner_strip():
    strat = AutoFormParsingStrategy()
    info = FormInfo(
        id="i",
        name="5ФК",
        type=FormType.FK_5,
        requisites={
            "skip_sheets": [0],
            "horizontal_header_strip_fk1_banner": True,
        },
    )
    assert strat.get_horizontal_header_strip_fk1_banner("Раздел1", info) is True
