DROP DATABASE IF EXISTS spark_streaming_schema;
CREATE DATABASE spark_streaming_schema;

\c spark_streaming_schema;

CREATE TABLE dim_product
(
    product_id         VARCHAR(255) PRIMARY KEY,
    suffix             TEXT,
    product_name       TEXT,
    sku                TEXT,
    attribute_set_id   INTEGER,
    type_id            TEXT,
    min_price          NUMERIC,
    max_price          NUMERIC,
    collection_id      TEXT,
    product_type_value TEXT,
    product_subtype_id INTEGER,
    store_code         TEXT,
    gender             TEXT
);


CREATE TABLE dim_store
(
    store_id   VARCHAR(255) PRIMARY KEY,
    store_name VARCHAR(255)
);

CREATE TABLE dim_location
(
    location_id   VARCHAR(255) PRIMARY KEY,
    country_name  VARCHAR(255),
    country_short VARCHAR(100),
    region_name   VARCHAR(255),
    city_name     VARCHAR(255)
);


CREATE TABLE dim_device
(
    device_id     VARCHAR(255) PRIMARY KEY,
    user_agent    TEXT,
    resolution    VARCHAR(50),
    os            VARCHAR(100),
    browser       VARCHAR(100)
);

CREATE TABLE dim_customer
(
    customer_id   VARCHAR(255) PRIMARY KEY,
    email_address VARCHAR(255),
    user_id_db    VARCHAR(255)
);

CREATE TABLE dim_date
(
    date_id               INTEGER PRIMARY KEY,
    full_date             DATE NOT NULL,
    date_of_week          VARCHAR(20),
    date_of_week_short    VARCHAR(10),
    is_weekday_or_weekend VARCHAR(10),
    day_of_month          INTEGER,
    day_of_year           INTEGER,
    week_of_year          INTEGER,
    quarter_number        INTEGER,
    year_number           INTEGER,
    year_month            VARCHAR(10)
);


CREATE TABLE fact_product_views
(
    fact_id      VARCHAR(255) PRIMARY KEY,
    product_id   VARCHAR(255),
    store_id     VARCHAR(255),
    location_id  VARCHAR(255),
    customer_id  VARCHAR(255),
    device_id    VARCHAR(255),
    date_id      INTEGER,
    ip_address   VARCHAR(50),
    time_stamp   TIMESTAMP,
    collection   TEXT,
    current_url  TEXT,
    referrer_url TEXT,
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES dim_product (product_id),
    CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES dim_store (store_id),
    CONSTRAINT fk_location FOREIGN KEY (location_id) REFERENCES dim_location (location_id),
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES dim_customer (customer_id),
    CONSTRAINT fk_device FOREIGN KEY (device_id) REFERENCES dim_device (device_id),
    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES dim_date (date_id)
);
