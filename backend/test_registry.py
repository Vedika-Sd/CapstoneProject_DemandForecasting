from services.registry_loader import registry_loader


products = registry_loader.get_all_products()

print(products[:5])

config = registry_loader.get_product_config(
    "Premium (Past. Cow Milk)"
)

print(config)