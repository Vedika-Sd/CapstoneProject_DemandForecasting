import json
from pathlib import Path

from core.config import settings


class RegistryLoader:

    def __init__(self):

        self.registry_path = Path(settings.MODEL_REGISTRY_PATH)

        self.registry = self._load_registry()

    def _load_registry(self):

        if not self.registry_path.exists():
            raise FileNotFoundError(
                f"Registry file not found: {self.registry_path}"
            )

        with open(self.registry_path, "r", encoding="utf-8") as f:

            registry = json.load(f)

        return registry

    def get_all_products(self):

        return list(self.registry["products"].keys())

    def get_product_config(self, product_name: str):

        products = self.registry["products"]

        if product_name not in products:
            raise ValueError(f"Product not found: {product_name}")

        return products[product_name]

    def get_winner_model(self, product_name: str):

        config = self.get_product_config(product_name)

        return config["winner_model"]


registry_loader = RegistryLoader()