WITH _u_0 AS (
  SELECT
    Countrycode AS _u_1
  FROM main.CountryNotes
  WHERE
    Seriescode = 'DT.DOD.DECT.CD'
  GROUP BY
    1
)
SELECT
  Country.CountryCode AS country_code
FROM main.Country AS Country
LEFT JOIN _u_0 AS _u_0
  ON Country.CountryCode = _u_0._u_1
WHERE
  Country.IncomeGroup = 'Low income' AND NOT _u_0._u_1 IS NULL
