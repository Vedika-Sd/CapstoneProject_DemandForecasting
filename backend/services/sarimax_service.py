from pathlib import Path
import joblib
import pandas as pd

from core.config import settings
from services.registry_loader import registry_loader


class SarimaxForecastService:

    def __init__(self):

        self.models_cache = {}

    def load_model(self, model_path: str):

        full_path = (
            Path(settings.SAVED_MODELS_DIR)
            / Path(model_path).relative_to("saved_models")
        )

        if str(full_path) in self.models_cache:
            return self.models_cache[str(full_path)]

        if not full_path.exists():
            raise FileNotFoundError(
                f"SARIMAX model not found: {full_path}"
            )

        model = joblib.load(full_path)

        self.models_cache[str(full_path)] = model

        return model

    def forecast(self, product_name: str, days: int = 30):

        config = registry_loader.get_product_config(product_name)

        if config["winner_model"] != "SARIMAX":

            raise ValueError(
                f"{product_name} is not assigned to SARIMAX model."
            )

        model = self.load_model(config["model_path"])

        preds = model.forecast(steps=days)

        future_dates = pd.date_range(
            start=pd.Timestamp.today().normalize(),
            periods=days,
            freq="D"
        )

        forecasts = []

        for date, pred in zip(future_dates, preds):

            forecasts.append({
                "date": date.strftime("%Y-%m-%d"),
                "forecast": round(max(float(pred), 0), 2)
            })

        return {
            "product": product_name,
            "model_used": "SARIMAX",
            "segment": config["segment"],
            "forecast_days": days,
            "forecasts": forecasts,
            "mean_daily_demand": config.get("mean_daily"),
            "wape": config.get("wape"),
            "smape": config.get("smape"),
            "mae": config.get("mae"),
            "grade": config.get("grade")
        }


sarimax_service = SarimaxForecastService()