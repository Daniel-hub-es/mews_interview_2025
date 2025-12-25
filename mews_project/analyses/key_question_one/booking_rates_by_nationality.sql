SELECT distinct on (nationality_code)
    rate_name, 
    nationality_code, 
    COUNT(*) AS booking_rate_volume
FROM fct__rate_popularity
WHERE nationality_code IS NOT NULL
GROUP BY rate_name, nationality_code
ORDER BY nationality_code, booking_rate_volume desc