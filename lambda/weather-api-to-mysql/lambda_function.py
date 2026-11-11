import json
import os
from datetime import datetime

import pymysql


def get_connection():
    return pymysql.connect(
        host=os.environ["DB_HOST"],
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"],
        connect_timeout=int(os.getenv("DB_CONNECT_TIMEOUT", "10")),
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


INSERT_SQL = """
INSERT INTO weather_observations (
    city, city_id, country, latitude, longitude,
    observed_at, ingested_at, temperature, feels_like,
    temp_min, temp_max, pressure, humidity,
    sea_level_pressure, ground_level_pressure, visibility,
    wind_speed, wind_direction, wind_gust, cloudiness,
    weather_id, weather_main, weather_description, weather_icon,
    sunrise, sunset, timezone_offset, source
) VALUES (
    %(city)s, %(city_id)s, %(country)s, %(latitude)s, %(longitude)s,
    %(observed_at)s, %(ingested_at)s, %(temperature)s, %(feels_like)s,
    %(temp_min)s, %(temp_max)s, %(pressure)s, %(humidity)s,
    %(sea_level_pressure)s, %(ground_level_pressure)s, %(visibility)s,
    %(wind_speed)s, %(wind_direction)s, %(wind_gust)s, %(cloudiness)s,
    %(weather_id)s, %(weather_main)s, %(weather_description)s, %(weather_icon)s,
    %(sunrise)s, %(sunset)s, %(timezone_offset)s, %(source)s
)
"""


def to_mysql_datetime(value):
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=None)


def lambda_handler(event, context):
    records = event.get("Records", [])
    if not records:
        return {"statusCode": 200, "inserted": 0}

    connection = get_connection()
    inserted = 0

    try:
        with connection.cursor() as cursor:
            for record in records:
                payload = json.loads(record["body"])
                payload["observed_at"] = to_mysql_datetime(payload["observed_at"])
                payload["ingested_at"] = to_mysql_datetime(payload["ingested_at"])
                payload["sunrise"] = to_mysql_datetime(payload.get("sunrise"))
                payload["sunset"] = to_mysql_datetime(payload.get("sunset"))
                cursor.execute(INSERT_SQL, payload)
                inserted += 1

        connection.commit()
        print(f"Inserted {inserted} weather observations")
        return {"statusCode": 200, "inserted": inserted}

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()
