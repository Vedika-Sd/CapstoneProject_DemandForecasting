from pathlib import Path
import json

import numpy as np
import pandas as pd
import xgboost as xgb

from core.config import settings
from services.registry_loader import registry_loader
from services.feature_builder import build_xgb_features


class XGBoostForecastService:

    def __init__(self):

        self.models_cache = {}

    def load_model(self, model_path):

        full_path = (
            Path(settings.SAVED_MODELS_DIR)
            / Path(model_path).relative_to("saved_models")
        )

        if str(full_path) in self.models_cache:
            return self.models_cache[str(full_path)]

        model = xgb.XGBRegressor()

        model.load_model(full_path)

        self.models_cache[str(full_path)] = model

        return model

    def load_tail_data(self, tail_path):

        full_path = (
            Path(settings.SAVED_MODELS_DIR)
            / Path(tail_path).relative_to("saved_models")
        )

        df = pd.read_csv(full_path)

        df["ds"] = pd.to_datetime(df["ds"])

        return df

    def load_feature_columns(self, features_path):

        full_path = (
            Path(settings.SAVED_MODELS_DIR)
            / Path(features_path).relative_to("saved_models")
        )

        with open(full_path, "r") as f:

            cols = json.load(f)

        return cols

    def recursive_forecast(
        self,
        model,
        history_df,
        feature_cols,
        known_festivals,
        days
    ):

        df = history_df.copy()

        forecasts = []

        for _ in range(days):

            next_date = (
                df["ds"].max()
                + pd.Timedelta(days=1)
            )

            next_row = pd.DataFrame({
                "ds": [next_date],
                "y": [np.nan]
            })

            df = pd.concat(
                [df, next_row],
                ignore_index=True
            )

            df = build_xgb_features(
                df,
                known_festivals
            )

            latest_row = (
                df.iloc[-1:][feature_cols]
            )

            pred = model.predict(latest_row)[0]

            pred = max(float(pred), 0)

            df.loc[df.index[-1], "y"] = pred

            forecasts.append({
                "date": next_date.strftime("%Y-%m-%d"),
                "forecast": round(pred, 2)
            })

        return forecasts

    def forecast(
        self,
        product_name,
        days=30
    ):

        config = registry_loader.get_product_config(
            product_name
        )

        if config["winner_model"] != "XGBoost":

            raise ValueError(
                f"{product_name} is not assigned to XGBoost."
            )

        model = self.load_model(
            config["model_path"]
        )

        tail_df = self.load_tail_data(
            config["tail_path"]
        )

        feature_cols = self.load_feature_columns(
            config["feature_cols_path"]
        )

        known_festivals = registry_loader.registry[
            "known_festivals"
        ]

        forecasts = self.recursive_forecast(
            model=model,
            history_df=tail_df,
            feature_cols=feature_cols,
            known_festivals=known_festivals,
            days=days
        )

        return {
            "product": product_name,
            "model_used": "XGBoost",
            "segment": config["segment"],
            "forecast_days": days,
            "forecasts": forecasts,
            "mean_daily_demand": config.get("mean_daily"),
            "wape": config.get("wape"),
            "smape": config.get("smape"),
            "mae": config.get("mae"),
            "grade": config.get("grade")
        }


xgboost_service = XGBoostForecastService()