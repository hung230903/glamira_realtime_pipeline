import os
import logging
import hashlib
import IP2Location


def get_loc_info(ip_address, db_path):
    try:
        # Check if the provided path exists
        actual_path = db_path
        if not os.path.exists(actual_path):
            # If not, check if the file exists in the current directory (where Spark addFile puts it)
            filename = os.path.basename(db_path)
            if os.path.exists(filename):
                actual_path = filename
            else:
                logging.warning(f"IP2LOC_DB NOT FOUND at {db_path} or {filename}")
                return None

        ipdb = IP2Location.IP2Location(actual_path)
        info = ipdb.get_all(ip_address)

        if info:
            country = info.country_long if info.country_long else ""
            region = info.region if info.region else ""
            city = info.city if info.city else ""

            loc_string = f"{country}|{region}|{city}"
            loc_id = hashlib.md5(loc_string.encode('utf-8')).hexdigest()

            return (
                loc_id,
                info.country_long,
                info.country_short,
                info.region,
                info.city
            )

    except Exception as e:
        logging.error(ip_address, "IS NOT FOUND")
        return None
    return None
