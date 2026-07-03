from typing import List, Optional

from pydantic import BaseModel


class ForecastPoint(BaseModel):

    date: str

    forecast: float


class ForecastResponse(BaseModel):

    product: str

    model_used: str

    segment: str

    forecast_days: int

    forecasts: List[ForecastPoint]

    mean_daily_demand: Optional[float] = None

    wape: Optional[float] = None

    smape: Optional[float] = None

    mae: Optional[float] = None

    grade: Optional[str] = None


class ErrorResponse(BaseModel):

    detail: str