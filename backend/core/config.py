from pathlib import Path
from pydantic_settings import BaseSettings


BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):

    APP_NAME: str = "Krushna Dairy Forecast API"

    MODEL_REGISTRY_PATH: str = str(BASE_DIR / "model_registry.json")

    SAVED_MODELS_DIR: str = str(BASE_DIR / "saved_models")

    FESTIVALS_PATH: str = str(BASE_DIR / "all_festivals.csv")

    UPLOAD_DIR: str = str(BASE_DIR / "data" / "uploads")

    FORECAST_DAYS_DEFAULT: int = 30


settings = Settings()