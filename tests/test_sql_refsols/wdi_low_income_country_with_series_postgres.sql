WITH _u_0 AS (
  SELECT
    countrycode AS _u_1
  FROM main.countrynotes
  WHERE
    seriescode = 'DT.DOD.DECT.CD'
  GROUP BY
    1
)
SELECT
  country.countrycode AS country_code
FROM main.country AS country
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = country.countrycode
WHERE
  NOT _u_0._u_1 IS NULL AND country.incomegroup = 'Low income'
