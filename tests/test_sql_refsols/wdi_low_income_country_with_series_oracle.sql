WITH "_u_0" AS (
  SELECT
    countrycode AS "_u_1"
  FROM MAIN.COUNTRYNOTES
  WHERE
    seriescode = 'DT.DOD.DECT.CD'
  GROUP BY
    countrycode
)
SELECT
  COUNTRY.countrycode AS country_code
FROM MAIN.COUNTRY COUNTRY
LEFT JOIN "_u_0" "_u_0"
  ON COUNTRY.countrycode = "_u_0"."_u_1"
WHERE
  COUNTRY.incomegroup = 'Low income' AND NOT "_u_0"."_u_1" IS NULL
