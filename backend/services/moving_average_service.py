import pandas as pd

from services.registry_loader import registry_loader


class MovingAverageForecastService:

    def forecast(self, product_name: str, days: int = 30):

        config = registry_loader.get_product_config(product_name)

        if config["winner_model"] != "MovingAverage":

            raise ValueError(
                f"{product_name} is not assigned to MovingAverage model."
            )

        ma_value = config.get("ma_forecast", 0)

        future_dates = pd.date_range(
            start=pd.Timestamp.today().normalize(),
            periods=days,
            freq="D"
        )

        forecasts = []

        for date in future_dates:

            forecasts.append({
                "date": date.strftime("%Y-%m-%d"),
                "forecast": round(float(ma_value), 2)
            })

        return {
            "product": product_name,
            "model_used": "MovingAverage",
            "segment": config["segment"],
            "forecast_days": days,
            "forecasts": forecasts,
            "mean_daily_demand": config.get("mean_daily"),
            "wape": config.get("wape"),
            "smape": config.get("smape"),
            "mae": config.get("mae"),
            "grade": config.get("grade"),
            "min_stock_buffer": config.get("min_stock_buffer"),
            "note": config.get("note")
        }


moving_average_service = MovingAverageForecastService()