import json
from pathlib import Path

import pandas as pd
from prophet.serialize import model_from_json

from core.config import settings
from services.registry_loader import registry_loader


class ProphetForecastService:

    def __init__(self):

        self.models_cache = {}

    def load_model(self, model_path: str):

        full_path = Path(settings.SAVED_MODELS_DIR) / Path(model_path).relative_to("saved_models")

        if str(full_path) in self.models_cache:
            return self.models_cache[str(full_path)]

        if not full_path.exists():
            raise FileNotFoundError(f"Prophet model not found: {full_path}")

        with open(full_path, "r") as f:

            model = model_from_json(f.read())

        self.models_cache[str(full_path)] = model

        return model

    def forecast(self, product_name: str, days: int = 30):

        config = registry_loader.get_product_config(product_name)

        if config["winner_model"] != "Prophet":
            raise ValueError(
                f"{product_name} is not assigned to Prophet model."
            )

        model = self.load_model(config["model_path"])

        future = model.make_future_dataframe(
            periods=days
        )

        forecast_df = model.predict(future)

        forecast_only = forecast_df.tail(days)[["ds", "yhat"]]

        forecasts = []

        for _, row in forecast_only.iterrows():

            forecasts.append({
                "date": row["ds"].strftime("%Y-%m-%d"),
                "forecast": round(max(row["yhat"], 0), 2)
            })

        return {
            "product": product_name,
            "model_used": "Prophet",
            "segment": config["segment"],
            "forecast_days": days,
            "forecasts": forecasts,
            "mean_daily_demand": config.get("mean_daily"),
            "wape": config.get("wape"),
            "smape": config.get("smape"),
            "mae": config.get("mae"),
            "grade": config.get("grade")
        }


prophet_service = ProphetForecastService()