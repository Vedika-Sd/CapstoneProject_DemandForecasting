from fastapi import FastAPI

from routers.forecast import router as forecast_router


app = FastAPI(
    title="Krushna Dairy Forecast API"
)


app.include_router(
    forecast_router
)

# app.include_router(products_router)
# app.include_router(festivals_router)
# app.include_router(whatif_router)
# app.include_router(upload_router)