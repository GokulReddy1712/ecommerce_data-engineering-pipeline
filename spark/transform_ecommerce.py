
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

# 1. Create Spark session with optimized settings
spark = SparkSession.builder \
    .appName("EcommerceDataTransformation") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memoryOverhead", "512m") \
    .getOrCreate()

# 2. GCS bucket
BUCKET = "ecommerce-raw-data-3f0332f0"

# 3. Read raw CSV files
# Schema inference is disabled to reduce unnecessary processing.

customers = spark.read.option("header", True).csv(
    f"gs://{BUCKET}/raw/customers/customers.csv"
)

products = spark.read.option("header", True).csv(
    f"gs://{BUCKET}/raw/products/products.csv"
)

orders = spark.read.option("header", True).csv(
    f"gs://{BUCKET}/raw/orders/orders.csv"
)

order_items = spark.read.option("header", True).csv(
    f"gs://{BUCKET}/raw/order_items/order_items.csv"
)

# 4. Clean column values and remove duplicate rows
def clean_data(df):
    for column in df.columns:
        df = df.withColumn(
            column,
            trim(col(column).cast("string"))
        )

    return df.dropDuplicates()


customers = clean_data(customers)
products = clean_data(products)
orders = clean_data(orders)
order_items = clean_data(order_items)

# 5. Write curated datasets to GCS

customers.write.mode("overwrite").option("header", True).csv(
    f"gs://{BUCKET}/curated/customers/"
)

products.write.mode("overwrite").option("header", True).csv(
    f"gs://{BUCKET}/curated/products/"
)

orders.write.mode("overwrite").option("header", True).csv(
    f"gs://{BUCKET}/curated/orders/"
)

order_items.write.mode("overwrite").option("header", True).csv(
    f"gs://{BUCKET}/curated/order_items/"
)

print("E-commerce data transformation completed successfully!")

# 6. Stop Spark session
spark.stop()
