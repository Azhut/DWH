from typing import Dict, Type
from app.services.form_processor_base import FormProcessorBase


class FormProcessorRegistry:
    _registry: Dict[str, Type[FormProcessorBase]] = {}

    @classmethod
    def register(cls, processor_cls: Type[FormProcessorBase]):
        cls._registry[processor_cls.form_code] = processor_cls

    @classmethod
    def get_processor(cls, form_code: str) -> Type[FormProcessorBase]:
        if form_code not in cls._registry:
            raise ValueError(f"Нет обработчика для формы {form_code}")
        return cls._registry[form_code]

    @classmethod
    def all_registered_forms(cls):
        return cls._registry
