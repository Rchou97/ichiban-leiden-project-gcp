SELECT
    tb.date,
    tb.author,
    tb.review,
    tb.quality AS rating,
    tb.delivery_time AS delivery_rating,
    'https://www.thuisbezorgd.nl/menu/ichi-ban-1#beoordelingen' AS link,
    'True' AS local_guide,
    FORMAT_DATE('%A', tb.date) AS day,
    EXTRACT(WEEK FROM tb.date) AS week_number,
    FORMAT_DATE('%B', tb.date) AS month,
    EXTRACT(DAY FROM tb.date) AS day_of_month,
    'thuisbezorgd_reviews' AS source
FROM `thuisbezorgd_reviews` AS tb
WHERE tb.date > DATE '2023-05-01'

UNION ALL

SELECT
    g.date,
    g.author,
    g.review,
    g.rating,
    NULL AS delivery_rating,
    g.link,
    CASE g.local_guide
        WHEN 'True' THEN 'True'
        ELSE 'False'
    END AS local_guide,
    FORMAT_DATE('%A', g.date) AS day,
    EXTRACT(WEEK FROM g.date) AS week_number,
    FORMAT_DATE('%B', g.date) AS month,
    EXTRACT(DAY FROM g.date) AS day_of_month,
    g.source
FROM `google_reviews` AS g
WHERE g.date > DATE '2023-05-01';