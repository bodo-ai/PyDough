WITH "_s0" AS (
  SELECT
    CITY_NAME,
    REGION
  FROM MAIN.GEOGRAPHIC
), "_s1" AS (
  SELECT
    CITY_NAME
  FROM MAIN.RESTAURANT
), "_u_0" AS (
  SELECT
    CITY_NAME AS "_u_1"
  FROM "_s1"
  GROUP BY
    CITY_NAME
), "_s6" AS (
  SELECT DISTINCT
    "_s0".REGION
  FROM "_s0" "_s0"
  LEFT JOIN "_u_0" "_u_0"
    ON "_s0".CITY_NAME = "_u_0"."_u_1"
  WHERE
    NOT "_u_0"."_u_1" IS NULL
), "_u_2" AS (
  SELECT
    CITY_NAME AS "_u_3"
  FROM "_s1"
  GROUP BY
    CITY_NAME
), "_s5" AS (
  SELECT
    CITY_NAME,
    COUNT(RATING) AS COUNT_RATING,
    SUM(RATING) AS SUM_RATING
  FROM MAIN.RESTAURANT
  GROUP BY
    CITY_NAME
), "_s7" AS (
  SELECT
    SUM("_s5".SUM_RATING) / SUM("_s5".COUNT_RATING) AS AVG_RATING,
    "_s2".REGION
  FROM "_s0" "_s2"
  LEFT JOIN "_u_2" "_u_2"
    ON "_s2".CITY_NAME = "_u_2"."_u_3"
  JOIN "_s5" "_s5"
    ON "_s2".CITY_NAME = "_s5".CITY_NAME
  WHERE
    NOT "_u_2"."_u_3" IS NULL
  GROUP BY
    "_s2".REGION
)
SELECT
  "_s6".REGION AS rest_region,
  "_s7".AVG_RATING AS avg_rating
FROM "_s6" "_s6"
LEFT JOIN "_s7" "_s7"
  ON "_s6".REGION = "_s7".REGION
ORDER BY
  1 NULLS FIRST
