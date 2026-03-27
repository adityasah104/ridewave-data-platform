-- Load rides (replace YOUR_ACCOUNT_ID and YOUR_NAME)
COPY rides_YOUR_NAME
FROM 's3://s3-de-q1-26/DE-Training/Day10/YOUR_NAME/raw/rides/'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/de-redshift-role'
CSV
IGNOREHEADER 1
DATEFORMAT 'YYYY-MM-DD'
REGION 'us-east-1';

-- Load drivers
COPY drivers_YOUR_NAME
FROM 's3://s3-de-q1-26/DE-Training/Day10/YOUR_NAME/raw/drivers/'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/de-redshift-role'
CSV
IGNOREHEADER 1
DATEFORMAT 'YYYY-MM-DD'
REGION 'us-east-1';

-- Verify row counts
SELECT 'rides'   AS tbl, COUNT(*) AS rows FROM rides_YOUR_NAME
UNION ALL
SELECT 'drivers' AS tbl, COUNT(*) AS rows FROM drivers_YOUR_NAME;




SELECT
    city,
    DATE_TRUNC('month', ride_date)      AS month,
    COUNT(*)                             AS total_rides,
    ROUND(SUM(fare_amount), 2)           AS monthly_revenue
FROM rides_YOUR_NAME
WHERE fare_amount IS NOT NULL
GROUP BY city, DATE_TRUNC('month', ride_date)
ORDER BY city, month;

-- Standard 2: Driver performance by city
-- Business use: HR team identifies top performers for bonuses
SELECT
    d.city,
    ROUND(AVG(d.rating), 2)             AS avg_driver_rating,
    SUM(d.total_rides)                   AS total_rides_in_city,
    COUNT(DISTINCT d.driver_id)          AS active_drivers
FROM drivers_YOUR_NAME d
WHERE d.is_active = 'Y'
GROUP BY d.city
ORDER BY total_rides_in_city DESC;



-- S2a: DENSE_RANK drivers by fare per city
-- Business use: Leaderboard — top 2 drivers per city for rewards
WITH driver_fares AS (
    SELECT driver_id, city,
           ROUND(SUM(fare_amount),2) AS total_fare
    FROM rides_YOUR_NAME
    WHERE fare_amount IS NOT NULL
    GROUP BY driver_id, city
)
SELECT *,
    DENSE_RANK() OVER (
        PARTITION BY city ORDER BY total_fare DESC
    ) AS city_rank
FROM driver_fares
WHERE city_rank <= 2
ORDER BY city, city_rank;

-- S2b: Drivers earning above city average
-- Business use: Identify high performers for premium tier
SELECT
    r.driver_id, r.city,
    ROUND(SUM(r.fare_amount),2)  AS total_fare,
    ROUND(AVG(r2.fare_amount),2) AS city_avg_fare
FROM rides_YOUR_NAME r
JOIN rides_YOUR_NAME r2 ON r.city = r2.city
WHERE r.fare_amount IS NOT NULL
  AND r2.fare_amount IS NOT NULL
GROUP BY r.driver_id, r.city
HAVING SUM(r.fare_amount) > AVG(r2.fare_amount)
ORDER BY r.city, total_fare DESC;




-- S2c: Revenue bucket classification
-- Business use: Segment rides for pricing strategy
SELECT
    CASE
        WHEN fare_amount > 500  THEN 'Premium'
        WHEN fare_amount >= 200 THEN 'Standard'
        ELSE 'Budget'
    END                            AS fare_bucket,
    COUNT(*)                       AS ride_count,
    ROUND(SUM(fare_amount),2)      AS total_revenue
FROM rides_YOUR_NAME
WHERE fare_amount IS NOT NULL
GROUP BY 1
ORDER BY total_revenue DESC;