# check_pydantic_migration.py
import pydantic

print(f"Pydantic version: {pydantic.__version__}")


# Проверяем старый синтаксис
class OldModel(pydantic.BaseModel):
    name: str

    class Config:
        extra = "ignore"


# Проверяем новый синтаксис
try:
    from pydantic import ConfigDict


    class NewModel(pydantic.BaseModel):
        name: str
        model_config = ConfigDict(extra="ignore")


    print("✓ Новый синтаксис работает")
except Exception as e:
    print(f"✗ Ошибка с новым синтаксисом: {e}")