import pandas as pd

from services.registry_loader import registry_loader


class CrostonForecastService:

    def forecast(self, product_name: str, days: int = 30):

        config = registry_loader.get_product_config(product_name)

        if config["winner_model"] != "Croston":

            raise ValueError(
                f"{product_name} is not assigned to Croston model."
            )

        croston_value = config.get("croston_estimate", 0)

        future_dates = pd.date_range(
            start=pd.Timestamp.today().normalize(),
            periods=days,
            freq="D"
        )

        forecasts = []

        for date in future_dates:

            forecasts.append({
                "date": date.strftime("%Y-%m-%d"),
                "forecast": round(float(croston_value), 2)
            })

        return {
            "product": product_name,
            "model_used": "Croston",
            "segment": config["segment"],
            "forecast_days": days,
            "forecasts": forecasts,
            "mean_daily_demand": config.get("mean_daily"),
            "zero_day_pct": config.get("zero_day_pct"),
            "croston_estimate": config.get("croston_estimate"),
            "wape": config.get("wape"),
            "smape": config.get("smape"),
            "mae": config.get("mae"),
            "grade": config.get("grade")
        }


croston_service = CrostonForecastService()