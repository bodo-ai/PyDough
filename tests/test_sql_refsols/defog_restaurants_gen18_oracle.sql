WITH "_S0" AS (
  SELECT
    city_name AS CITY_NAME,
    region AS REGION
  FROM MAIN.GEOGRAPHIC
), "_S1" AS (
  SELECT
    city_name AS CITY_NAME
  FROM MAIN.RESTAURANT
), "_u_0" AS (
  SELECT
    CITY_NAME AS "_u_1"
  FROM "_S1"
  GROUP BY
    CITY_NAME
), "_S6" AS (
  SELECT DISTINCT
    "_S0".REGION
  FROM "_S0" "_S0"
  LEFT JOIN "_u_0" "_u_0"
    ON "_S0".CITY_NAME = "_u_0"."_u_1"
  WHERE
    NOT "_u_0"."_u_1" IS NULL
), "_u_2" AS (
  SELECT
    CITY_NAME AS "_u_3"
  FROM "_S1"
  GROUP BY
    CITY_NAME
), "_S5" AS (
  SELECT
    city_name AS CITY_NAME,
    COUNT(rating) AS COUNT_RATING,
    SUM(rating) AS SUM_RATING
  FROM MAIN.RESTAURANT
  GROUP BY
    city_name
), "_S7" AS (
  SELECT
    SUM("_S5".SUM_RATING) / SUM("_S5".COUNT_RATING) AS AVG_RATING,
    "_S2".REGION
  FROM "_S0" "_S2"
  LEFT JOIN "_u_2" "_u_2"
    ON "_S2".CITY_NAME = "_u_2"."_u_3"
  JOIN "_S5" "_S5"
    ON "_S2".CITY_NAME = "_S5".CITY_NAME
  WHERE
    NOT "_u_2"."_u_3" IS NULL
  GROUP BY
    "_S2".REGION
)
SELECT
  "_S6".REGION AS rest_region,
  "_S7".AVG_RATING AS avg_rating
FROM "_S6" "_S6"
LEFT JOIN "_S7" "_S7"
  ON "_S6".REGION = "_S7".REGION
ORDER BY
  1 NULLS FIRST
