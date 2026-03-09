from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple


@dataclass(frozen=True)
class SystemFormSpec:
    """
    Спецификация «системной» формы.

    Такие формы должны существовать в БД, потому что на них завязаны стратегии парсинга.
    """

    name: str
    requisites: Dict[str, Any]


SYSTEM_FORMS: Tuple[SystemFormSpec, ...] = (
    SystemFormSpec(name="1ФК", requisites={"skip_sheets": [0]}),
    SystemFormSpec(name="5ФК", requisites={"skip_sheets": [0]}),
)

