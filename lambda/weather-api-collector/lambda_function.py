import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

sqs = boto3.client("sqs")
secrets = boto3.client("secretsmanager")

CITIES = [
    "Hyderabad,IN",
    "Bengaluru,IN",
    "Chennai,IN",
    "Mumbai,IN",
    "Delhi,IN",
]


def get_api_key():
    secret_name = os.environ["OPENWEATHER_SECRET_NAME"]
    response = secrets.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret["api_key"]


def get_weather(city, api_key):
    params = urllib.parse.urlencode({
        "q": city,
        "appid": api_key,
        "units": "metric",
    })
    url = f"https://api.openweathermap.org/data/2.5/weather?{params}"
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "weather-data-pipeline"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def normalize_weather(weather):
    observed_at = datetime.fromtimestamp(
        weather["dt"], tz=timezone.utc
    ).isoformat()

    return {
        "city": weather["name"],
        "city_id": weather["id"],
        "country": weather["sys"]["country"],
        "latitude": weather["coord"]["lat"],
        "longitude": weather["coord"]["lon"],
        "observed_at": observed_at,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "temperature": weather["main"]["temp"],
        "feels_like": weather["main"]["feels_like"],
        "temp_min": weather["main"]["temp_min"],
        "temp_max": weather["main"]["temp_max"],
        "pressure": weather["main"]["pressure"],
        "humidity": weather["main"]["humidity"],
        "sea_level_pressure": weather["main"].get("sea_level"),
        "ground_level_pressure": weather["main"].get("grnd_level"),
        "visibility": weather.get("visibility"),
        "wind_speed": weather.get("wind", {}).get("speed"),
        "wind_direction": weather.get("wind", {}).get("deg"),
        "wind_gust": weather.get("wind", {}).get("gust"),
        "cloudiness": weather.get("clouds", {}).get("all"),
        "weather_id": weather["weather"][0]["id"],
        "weather_main": weather["weather"][0]["main"],
        "weather_description": weather["weather"][0]["description"],
        "weather_icon": weather["weather"][0]["icon"],
        "sunrise": datetime.fromtimestamp(
            weather["sys"]["sunrise"], tz=timezone.utc
        ).isoformat(),
        "sunset": datetime.fromtimestamp(
            weather["sys"]["sunset"], tz=timezone.utc
        ).isoformat(),
        "timezone_offset": weather["timezone"],
        "source": "openweather",
    }


def lambda_handler(event, context):
    api_key = get_api_key()
    queue_url = os.environ["SQS_QUEUE_URL"]
    messages_sent = 0
    failures = []

    for city in CITIES:
        try:
            weather = get_weather(city, api_key)
            payload = normalize_weather(weather)

            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(payload),
            )
            messages_sent += 1
            print(f"Sent weather data for {city}")

        except Exception as exc:
            failures.append({"city": city, "error": str(exc)})
            print(f"Failed for {city}: {exc}")

    return {
        "statusCode": 200 if not failures else 207,
        "messages_sent": messages_sent,
        "failures": failures,
    }
