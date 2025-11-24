WITH _u_0 AS (
  SELECT
    countrycode AS _u_1
  FROM main.CountryNotes
  WHERE
    seriescode = 'DT.DOD.DECT.CD'
  GROUP BY
    1
)
SELECT
  Country.countrycode AS country_code
FROM main.Country AS Country
LEFT JOIN _u_0 AS _u_0
  ON Country.countrycode = _u_0._u_1
WHERE
  Country.incomegroup = 'Low income' AND NOT _u_0._u_1 IS NULL
