from fastapi import APIRouter, HTTPException

from schemas.forecast import ForecastResponse

from services.registry_loader import registry_loader
from services.prophet_service import prophet_service
from services.sarimax_service import sarimax_service
from services.moving_average_service import moving_average_service
from services.croston_service import croston_service
from services.xgboost_service import xgboost_service


router = APIRouter()


@router.get(
    "/forecast/{product_name}",
    response_model=ForecastResponse
)
def get_forecast(
    product_name: str,
    days: int = 30
):

    try:

        config = registry_loader.get_product_config(product_name)

        winner_model = config["winner_model"]

        if winner_model == "Prophet":

            return prophet_service.forecast(
                product_name,
                days
            )

        elif winner_model == "SARIMAX":

            return sarimax_service.forecast(
                product_name,
                days
            )

        elif winner_model == "MovingAverage":

            return moving_average_service.forecast(
                product_name,
                days
            )

        elif winner_model == "Croston":

            return croston_service.forecast(
                product_name,
                days
            )
        
        elif winner_model == "XGBoost":

            return xgboost_service.forecast(
                product_name,
                days
            )

        raise HTTPException(
            status_code=400,
            detail=f"{winner_model} service not implemented yet."
        )

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )