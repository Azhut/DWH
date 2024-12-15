from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URI: str = "mongodb://localhost:27017"
    DATABASE_NAME: str = "sport_data_new"


settings = Settings()
