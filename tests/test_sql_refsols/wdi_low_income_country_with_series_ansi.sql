WITH _s1 AS (
  SELECT
    countrycode
  FROM main.countrynotes
  WHERE
    seriescode = 'DT.DOD.DECT.CD'
)
SELECT
  country.countrycode AS country_code
FROM main.country AS country
SEMI JOIN _s1 AS _s1
  ON _s1.countrycode = country.countrycode
WHERE
  country.incomegroup = 'Low income'
