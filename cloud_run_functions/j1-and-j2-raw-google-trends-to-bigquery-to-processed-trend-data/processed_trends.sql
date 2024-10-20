WITH prep_dates AS (
  SELECT
    date,
    SPLIT(TRIM(REGEXP_REPLACE(date, r',\s*\d{4}', '')), '–') AS date_parts,
    REGEXP_EXTRACT(date, r'\d{4}') AS year
  FROM `gcp-ichiban-data-ingestion.ichiban_data_raw.google_trends`
),

parsed_dates AS (
SELECT
  date,
  PARSE_DATE('%Y-%m-%d',
    CONCAT(
      CASE
        WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Dec' AND SUBSTR(TRIM(date_parts[OFFSET(1)]), 1, 3) = 'Jan'
          THEN CAST(CAST(year AS INT64) - 1 AS STRING)
        ELSE year
      END,
      '-',
      CASE
        WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Jan' THEN '01'
        WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Feb' THEN '02'
        WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Mar' THEN '03'
        WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Apr' THEN '04'
        WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'May' THEN '05'
        WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Jun' THEN '06'
        WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Jul' THEN '07'
        WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Aug' THEN '08'
        WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Sep' THEN '09'
        WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Oct' THEN '10'
        WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Nov' THEN '11'
        WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Dec' THEN '12'
      END,
      '-',
      LPAD(REGEXP_EXTRACT(date_parts[OFFSET(0)], r'\d+'), 2, '0')
    )
  ) AS valid_from,
  PARSE_DATE('%Y-%m-%d',
    CONCAT(
      year,
      '-',
      CASE
        WHEN SUBSTR(TRIM(date_parts[OFFSET(1)]), 1, 3) = 'Jan' THEN '01'
        WHEN SUBSTR(TRIM(date_parts[OFFSET(1)]), 1, 3) = 'Feb' THEN '02'
        WHEN SUBSTR(TRIM(date_parts[OFFSET(1)]), 1, 3) = 'Mar' THEN '03'
        WHEN SUBSTR(TRIM(date_parts[OFFSET(1)]), 1, 3) = 'Apr' THEN '04'
        WHEN SUBSTR(TRIM(date_parts[OFFSET(1)]), 1, 3) = 'May' THEN '05'
        WHEN SUBSTR(TRIM(date_parts[OFFSET(1)]), 1, 3) = 'Jun' THEN '06'
        WHEN SUBSTR(TRIM(date_parts[OFFSET(1)]), 1, 3) = 'Jul' THEN '07'
        WHEN SUBSTR(TRIM(date_parts[OFFSET(1)]), 1, 3) = 'Aug' THEN '08'
        WHEN SUBSTR(TRIM(date_parts[OFFSET(1)]), 1, 3) = 'Sep' THEN '09'
        WHEN SUBSTR(TRIM(date_parts[OFFSET(1)]), 1, 3) = 'Oct' THEN '10'
        WHEN SUBSTR(TRIM(date_parts[OFFSET(1)]), 1, 3) = 'Nov' THEN '11'
        WHEN SUBSTR(TRIM(date_parts[OFFSET(1)]), 1, 3) = 'Dec' THEN '12'
        ELSE
          CASE
            WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Jan' THEN '01'
            WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Feb' THEN '02'
            WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Mar' THEN '03'
            WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Apr' THEN '04'
            WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'May' THEN '05'
            WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Jun' THEN '06'
            WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Jul' THEN '07'
            WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Aug' THEN '08'
            WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Sep' THEN '09'
            WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Oct' THEN '10'
            WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Nov' THEN '11'
            WHEN SUBSTR(date_parts[OFFSET(0)], 1, 3) = 'Dec' THEN '12'
          END
      END,
      '-',
      LPAD(REGEXP_EXTRACT(TRIM(date_parts[OFFSET(1)]), r'\d+'), 2, '0')
    )
  ) AS valid_to
FROM prep_dates
)

SELECT
  t1.valid_from,
  t1.valid_to,
  t2.extracted_value AS value,
  t2.query,
  t2.source
FROM parsed_dates AS t1
LEFT JOIN `gcp-ichiban-data-ingestion.ichiban_data_raw.google_trends` AS t2
  ON t1.date = t2.date
ORDER BY valid_from DESC;tab