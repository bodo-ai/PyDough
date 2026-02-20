WITH "_T1" AS (
  SELECT
    co_name AS CO_NAME
  FROM MAIN.COUNTRIES
  WHERE
    NOT co_name LIKE '%C%'
), "_T4" AS (
  SELECT
    pr_name AS PR_NAME,
    pr_release AS PR_RELEASE
  FROM MAIN.PRODUCTS
  WHERE
    pr_name = 'AmethystCopper-I'
), "_S3" AS (
  SELECT
    ca_dt AS CA_DT
  FROM MAIN.CALENDAR
), "_S15" AS (
  SELECT
    "_S7".CA_DT,
    "_T6".CO_NAME,
    COUNT(*) AS N_ROWS
  FROM "_T1" "_T6"
  CROSS JOIN "_T4" "_T7"
  JOIN "_S3" "_S7"
    ON "_S7".CA_DT < (
      CAST("_T7".PR_RELEASE AS TIMESTAMP) + NUMTOYMINTERVAL(2, 'year')
    )
    AND "_S7".CA_DT >= "_T7".PR_RELEASE
  JOIN MAIN.DEVICES DEVICES
    ON "_S7".CA_DT = TRUNC(CAST(DEVICES.de_purchase_ts AS TIMESTAMP), 'DAY')
  JOIN MAIN.PRODUCTS PRODUCTS
    ON DEVICES.de_product_id = PRODUCTS.pr_id AND PRODUCTS.pr_name = 'AmethystCopper-I'
  JOIN MAIN.COUNTRIES COUNTRIES
    ON COUNTRIES.co_id = DEVICES.de_purchase_country_id
    AND COUNTRIES.co_name = "_T6".CO_NAME
  GROUP BY
    "_S7".CA_DT,
    "_T6".CO_NAME
), "_S17" AS (
  SELECT
    TRUNC(CAST("_S3".CA_DT AS TIMESTAMP), 'YEAR') AS START_OF_YEAR,
    "_T3".CO_NAME,
    SUM("_S15".N_ROWS) AS SUM_N_ROWS
  FROM "_T1" "_T3"
  CROSS JOIN "_T4" "_T4"
  JOIN "_S3" "_S3"
    ON "_S3".CA_DT < (
      CAST("_T4".PR_RELEASE AS TIMESTAMP) + NUMTOYMINTERVAL(2, 'year')
    )
    AND "_S3".CA_DT >= "_T4".PR_RELEASE
  LEFT JOIN "_S15" "_S15"
    ON "_S15".CA_DT = "_S3".CA_DT AND "_S15".CO_NAME = "_T3".CO_NAME
  GROUP BY
    TRUNC(CAST("_S3".CA_DT AS TIMESTAMP), 'YEAR'),
    "_T3".CO_NAME
)
SELECT
  "_T1".CO_NAME AS country_name,
  "_S17".START_OF_YEAR AS start_of_year,
  COALESCE("_S17".SUM_N_ROWS, 0) AS n_purchases
FROM "_T1" "_T1"
LEFT JOIN "_S17" "_S17"
  ON "_S17".CO_NAME = "_T1".CO_NAME
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
