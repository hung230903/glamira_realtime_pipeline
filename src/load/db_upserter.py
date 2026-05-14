import logging


def upsert_location_dimension(conn, table, values):
    cur = conn.cursor()
    try:
        sql = """
              INSERT INTO dim_location (location_id, country_name, country_short, region_name, city_name)
              VALUES (%s, %s, %s, %s, %s) ON CONFLICT (location_id) DO
              UPDATE SET location_id = EXCLUDED.location_id
                  RETURNING location_id; \
              """
        cur.execute(sql, values)
        result = cur.fetchone()
        conn.commit()
        return result[0] if result else None
    except Exception as e:
        logging.error(f"ERROR WHEN UPSERTING {table}: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()


def upsert_product_dimension(conn, table, values):
    cur = conn.cursor()
    try:
        sql = """
              INSERT INTO dim_product (product_id)
              VALUES (%s) ON CONFLICT (product_id) DO
              UPDATE SET product_id = EXCLUDED.product_id
                  RETURNING product_id; \
              """
        cur.execute(sql, values)
        result = cur.fetchone()
        conn.commit()
        return result[0] if result else None
    except Exception as e:
        logging.error(f"ERROR WHEN UPSERTING {table}: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()


def upsert_store_dimension(conn, table, values):
    cur = conn.cursor()
    try:
        sql = """
              INSERT INTO dim_store (store_id, store_name)
              VALUES (%s, %s) ON CONFLICT (store_id) DO
              UPDATE
                  SET store_id = EXCLUDED.store_id RETURNING store_id; \
              """
        cur.execute(sql, values)

        result = cur.fetchone()
        conn.commit()
        return result[0] if result else None

    except Exception as e:
        logging.error(f"ERROR WHEN UPSERTING {table}: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()


def upsert_date_dimension(conn, table, values):
    cur = conn.cursor()
    try:
        sql = """
              INSERT INTO dim_date (date_id, full_date, date_of_week, date_of_week_short, is_weekday_or_weekend,
                                    day_of_month, day_of_year, week_of_year, quarter_number, year_number, year_month)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (date_id) DO NOTHING RETURNING date_id; \
              """
        cur.execute(sql, values)

        result = cur.fetchone()
        conn.commit()
        return result[0] if result else None

    except Exception as e:
        logging.error(f"ERROR WHEN UPSERTING {table}: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()


def upsert_device_dimension(conn, table, values):
    cur = conn.cursor()
    try:
        sql = """
              INSERT INTO dim_device (device_id, user_agent, resolution, os, browser)
              VALUES (%s, %s, %s, %s, %s) ON CONFLICT (device_id) DO NOTHING RETURNING device_id; \
              """
        cur.execute(sql, values)
        result = cur.fetchone()
        conn.commit()
        return result[0] if result else None
    except Exception as e:
        logging.error(f"ERROR WHEN UPSERTING {table}: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()


def upsert_customer_dimension(conn, table, values):
    cur = conn.cursor()
    try:
        sql = """
              INSERT INTO dim_customer (customer_id, email_address, user_id_db)
              VALUES (%s, %s, %s) ON CONFLICT (customer_id) DO
              UPDATE SET email_address = EXCLUDED.email_address,
                         user_id_db = EXCLUDED.user_id_db
                  RETURNING customer_id; \
              """
        cur.execute(sql, values)
        result = cur.fetchone()
        conn.commit()
        return result[0] if result else None
    except Exception as e:
        logging.error(f"ERROR WHEN UPSERTING {table}: {e}")
        conn.rollback()
        return None
    finally:
        cur.close()
