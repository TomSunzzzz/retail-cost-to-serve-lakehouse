"""Paths and the list of source tables, shared by every layer.

In Microsoft Fabric the only change is the root paths: RAW_DIR becomes "Files/raw"
and the layers are written as Lakehouse tables instead of folders.
"""

RAW_DIR = "data_set/archive"
LAKEHOUSE_DIR = "lakehouse"
BRONZE_DIR = f"{LAKEHOUSE_DIR}/bronze"
SILVER_DIR = f"{LAKEHOUSE_DIR}/silver"
GOLD_DIR = f"{LAKEHOUSE_DIR}/gold"
EXPORT_DIR = f"{LAKEHOUSE_DIR}/export"

# table name -> (source CSV file, primary key columns)
SOURCES = {
    "orders": ("olist_orders_dataset.csv", ["order_id"]),
    "order_items": ("olist_order_items_dataset.csv", ["order_id", "order_item_id"]),
    "order_payments": ("olist_order_payments_dataset.csv", ["order_id", "payment_sequential"]),
    "order_reviews": ("olist_order_reviews_dataset.csv", ["review_id", "order_id"]),
    "customers": ("olist_customers_dataset.csv", ["customer_id"]),
    "sellers": ("olist_sellers_dataset.csv", ["seller_id"]),
    "products": ("olist_products_dataset.csv", ["product_id"]),
    "geolocation": ("olist_geolocation_dataset.csv", ["geolocation_zip_code_prefix"]),
    "category_translation": ("product_category_name_translation.csv", ["product_category_name"]),
}
