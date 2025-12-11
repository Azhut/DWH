import pydantic

print(f"Pydantic version: {pydantic.__version__}")


class OldModel(pydantic.BaseModel):
    name: str

    class Config:
        extra = "ignore"


try:
    from pydantic import ConfigDict


    class NewModel(pydantic.BaseModel):
        name: str
        model_config = ConfigDict(extra="ignore")


    print("✓ Новый синтаксис работает")
except Exception as e:
    print(f"✗ Ошибка с новым синтаксисом: {e}")