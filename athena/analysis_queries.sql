-- Total observations
SELECT COUNT(*) AS total_observations
FROM weather;

-- Latest observation per city
SELECT city, MAX(observed_at) AS latest_observation
FROM weather
GROUP BY city
ORDER BY city;

-- Average temperature and humidity by city
SELECT
    city,
    ROUND(AVG(temperature), 2) AS avg_temperature,
    ROUND(AVG(humidity), 2) AS avg_humidity
FROM weather
GROUP BY city
ORDER BY avg_temperature DESC;

-- Temperature trend over time
SELECT
    city,
    date_trunc('hour', observed_at) AS observation_hour,
    ROUND(AVG(temperature), 2) AS avg_temperature
FROM weather
GROUP BY city, date_trunc('hour', observed_at)
ORDER BY observation_hour, city;

-- Check duplicate source IDs (if the crawler exposes id)
SELECT id, COUNT(*) AS row_count
FROM weather
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY row_count DESC;
