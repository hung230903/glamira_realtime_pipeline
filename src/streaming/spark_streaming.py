import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    date_format,
    from_unixtime,
    lit,
    sha2,
    when,
)
from pyspark.sql.types import StringType, StructField, StructType

import config.base as settings
from config.logger import setup_logger
from src.load.db_upserter import (
    upsert_customer_dimension,
    upsert_date_dimension,
    upsert_device_dimension,
    upsert_location_dimension,
    upsert_product_dimension,
    upsert_store_dimension,
)
from src.processing.data_transformer import (
    browser_transformer,
    customer_transformer,
    date_transformer,
    device_transformer,
    os_transformer,
    store_transformer,
)

logger = setup_logger("SparkStreaming")

# Lookup DF schema
DIM_SCHEMA = StructType(
    [
        StructField("map_id", StringType(), True),
        StructField("map_key", StringType(), True),
    ]
)


######################################################################
# 1. DIMENSION UPSERTS  (driver-side, single PG connection)
######################################################################
def upsert_all_dimensions(rows):
    conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        database=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
    )

    product_map, store_map, location_map = {}, {}, {}
    customer_map, date_map, device_map = {}, {}, {}

    for row in rows:
        # Transform and upsert PRODUCT
        if row.product_id and row.product_id not in product_map:
            logger.info("UPSERTING PRODUCT DIMENSION")
            pk = upsert_product_dimension(conn, "dim_product", (row.product_id,))
            product_map[row.product_id] = pk

        # Transform and upsert STORE
        if row.store_id and row.store_id not in store_map:
            store_name = store_transformer(row.store_id)
            store_tuple = (row.store_id, store_name)
            logger.info("UPSERTING STORE DIMENSION")
            sk = upsert_store_dimension(conn, "dim_store", store_tuple)
            store_map[row.store_id] = sk

        # Transform and upsert LOCATION
        if row.location_id and row.location_id not in location_map:
            loc_tuple = (
                row.location_id,
                row.country_name,
                row.country_short,
                row.region_name,
                row.city_name,
            )
            logger.info("UPSERTING LOCATION DIMENSION")
            lk = upsert_location_dimension(conn, "dim_location", loc_tuple)
            location_map[row.location_id] = lk

        # Transform and upsert CUSTOMER
        if row.device_id or row.email_address or row.user_id_db:
            customer_data = customer_transformer(
                customer_id=row.device_id,
                email_address=row.email_address,
                user_id_db=row.user_id_db,
            )

            if customer_data["customer_id"] not in customer_map:
                customer_tuple = (
                    customer_data["customer_id"],
                    customer_data["email_address"],
                    customer_data["user_id_db"],
                )
                logger.info("UPSERTING CUSTOMER DIMENSION")
                ck = upsert_customer_dimension(conn, "dim_customer", customer_tuple)
                customer_map[customer_data["customer_id"]] = ck

        # Transform and upsert DEVICE
        if row.user_agent or row.resolution:
            device_data = device_transformer(row.user_agent, row.resolution)
            if device_data["device_id"] not in device_map:
                browser = browser_transformer(row.user_agent)
                os_name = os_transformer(row.user_agent)
                device_tuple = (
                    device_data["device_id"],
                    device_data["user_agent"],
                    device_data["resolution"],
                    os_name,
                    browser,
                )
                logger.info("UPSERTING DEVICE DIMENSION")
                dk = upsert_device_dimension(conn, "dim_device", device_tuple)
                device_map[device_data["device_id"]] = dk

        # Transform and upsert DATE
        if row.time_stamp:
            date_data = date_transformer(row.time_stamp)

            if date_data:
                date_id = date_data["date_id"]
                if date_id not in date_map:
                    date_tuple = (
                        date_id,
                        date_data["full_date"],
                        date_data["date_of_week"],
                        date_data["date_of_week_short"],
                        date_data["is_weekday_or_weekend"],
                        date_data["day_of_month"],
                        date_data["day_of_year"],
                        date_data["week_of_year"],
                        date_data["quarter_number"],
                        date_data["year_number"],
                        date_data["year_month"],
                    )
                    logger.info("UPSERTING DATE DIMENSION")
                    upsert_date_dimension(conn, "dim_date", date_tuple)
                    date_map[date_id] = date_id

    conn.close()
    return product_map, store_map, location_map, customer_map, date_map, device_map


######################################################################
# 2. ENRICH BATCH (join batch_df with dimension maps → create fact_id)
######################################################################
def create_lookup_df(spark, mapping, id_col, key_col):
    """Dict → 2-column Spark DataFrame to join"""
    data = [(str(k), str(v) if v else None) for k, v in mapping.items()]
    return (
        spark.createDataFrame(data, schema=DIM_SCHEMA)
        .withColumnRenamed("map_id", id_col)
        .withColumnRenamed("map_key", key_col)
    )


def build_enriched_df(
    batch_df, product_map, store_map, location_map, customer_map, date_map, device_map
):
    """
    Join batch_df with lookup DataFrames, generate fact_id with SHA-256,
    and select columns for fact_product_views.
    """
    spark = SparkSession.builder.getOrCreate()

    # Handle timestamp in milliseconds (if > 10^10, assume ms)
    ts_col = when(col("time_stamp") > 9999999999, col("time_stamp") / 1000.0).otherwise(
        col("time_stamp")
    )

    prod_df = create_lookup_df(spark, product_map, "map_prod_id", "product_key")
    store_df = create_lookup_df(spark, store_map, "map_store_id", "store_key")
    loc_df = create_lookup_df(spark, location_map, "map_loc_id", "loc_key")
    cus_df = create_lookup_df(spark, customer_map, "map_cus_id", "cus_key")
    dev_df = create_lookup_df(spark, device_map, "map_dev_id", "dev_key")

    date_data = [(str(k), str(v) if v else None) for k, v in date_map.items()]
    date_df = (
        spark.createDataFrame(date_data, schema=DIM_SCHEMA)
        .withColumnRenamed("map_id", "date_id_str")
        .withColumnRenamed("map_key", "date_key")
    )

    enriched_df = (
        batch_df.join(prod_df, batch_df.product_id == prod_df.map_prod_id, "left")
        .join(store_df, batch_df.store_id == store_df.map_store_id, "left")
        .join(loc_df, batch_df.loc_info.location_id == loc_df.map_loc_id, "left")
        .join(cus_df, batch_df.device_id == cus_df.map_cus_id, "left")
        .withColumn(
            "device_id_hash",
            sha2(concat_ws("_", col("user_agent"), col("resolution")), 256),
        )
        .join(dev_df, col("device_id_hash") == dev_df.map_dev_id, "left")
        .join(
            date_df,
            date_format(from_unixtime(ts_col), "yyyyMMdd") == date_df.date_id_str,
            "left",
        )
        .withColumn(
            "fact_id",
            sha2(
                concat_ws(
                    "_",
                    coalesce(col("product_key"), lit("NA")),
                    coalesce(col("store_key"), lit("NA")),
                    coalesce(col("loc_key"), lit("NA")),
                    coalesce(col("cus_key"), lit("NA")),
                    coalesce(col("dev_key"), lit("NA")),
                    coalesce(col("date_key"), lit("NA")),
                    col("time_stamp").cast("string"),
                ),
                256,
            ),
        )
        .select(
            col("fact_id"),
            col("product_key").alias("product_id"),
            col("store_key").alias("store_id"),
            col("loc_key").alias("location_id"),
            col("cus_key").alias("customer_id"),
            col("dev_key").alias("device_id"),
            col("date_key").cast("integer").alias("date_id"),
            col("ip").alias("ip_address"),
            col("collection"),
            col("current_url"),
            col("referrer_url"),
            from_unixtime(ts_col).cast("timestamp").alias("time_stamp"),
        )
    )
    return enriched_df


######################################################################
# 3. FACT UPSERT
######################################################################
def write_fact_table(enriched_df):
    pg_host = settings.POSTGRES_HOST
    pg_port = settings.POSTGRES_PORT
    pg_db = settings.POSTGRES_DB
    pg_user = settings.POSTGRES_USER
    pg_pass = settings.POSTGRES_PASSWORD

    def upsert_fact_partition(rows):
        import psycopg2 as _pg
        from psycopg2.extras import execute_batch

        sql = """
              INSERT INTO fact_product_views (fact_id, product_id, store_id, location_id,
                                              customer_id, device_id, date_id, ip_address, time_stamp,
                                              collection, current_url, referrer_url)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (fact_id) DO NOTHING
              """

        conn = _pg.connect(
            host=pg_host,
            port=pg_port,
            database=pg_db,
            user=pg_user,
            password=pg_pass,
        )
        cur = conn.cursor()

        batch = [
            (
                r.fact_id,
                r.product_id,
                r.store_id,
                r.location_id,
                r.customer_id,
                r.device_id,
                r.date_id,
                r.ip_address,
                r.time_stamp,
                r.collection,
                r.current_url,
                r.referrer_url,
            )
            for r in rows
        ]

        if batch:
            execute_batch(cur, sql, batch, page_size=500)
            conn.commit()

        cur.close()
        conn.close()

    enriched_df.printSchema()
    enriched_df.foreachPartition(upsert_fact_partition)


######################################################################
# ORCHESTRATOR
######################################################################
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        logger.info(f"--- Batch {batch_id} is empty, skipping ---")
        return

    logger.info(f"--- Processing batch {batch_id} ---")
    batch_df.cache()

    count_raw = batch_df.count()
    logger.info(f"Batch {batch_id} has {count_raw} raw events")

    # Collect rows & upsert dimensions
    rows = batch_df.select(
        "collection",
        "current_url",
        "referrer_url",
        "product_id",
        "store_id",
        "user_agent",
        "user_id_db",
        "resolution",
        "time_stamp",
        "email_address",
        "device_id",
        "loc_info.location_id",
        "loc_info.country_name",
        "loc_info.country_short",
        "loc_info.region_name",
        "loc_info.city_name",
    ).collect()

    logger.info(
        f"Collected {len(rows)} rows from batch {batch_id} for dimension upserts."
    )

    product_map, store_map, location_map, customer_map, date_map, device_map = (
        upsert_all_dimensions(rows)
    )

    # Enrich batch with dimension keys
    enriched_df = build_enriched_df(
        batch_df,
        product_map,
        store_map,
        location_map,
        customer_map,
        date_map,
        device_map,
    )

    count_enriched = enriched_df.count()

    # Upsert data to fact table
    logger.info(f"Upserting {count_enriched} rows into fact_product_views")
    write_fact_table(enriched_df)

    batch_df.unpersist()
    logger.info(f"Batch {batch_id} completed.")
