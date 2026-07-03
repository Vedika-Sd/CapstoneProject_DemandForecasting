from typing import List

from pydantic import BaseModel


class ProductItem(BaseModel):

    product: str

    segment: str

    winner_model: str

    grade: str


class ProductListResponse(BaseModel):

    total_products: int

    products: List[ProductItem]