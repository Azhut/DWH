from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # DATABASE_URI: str = "mongodb://mongo:27017"
    DATABASE_URI: str = "localhost:27017"
    DATABASE_NAME: str = "sport_data"


settings = Settings()
