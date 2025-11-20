WITH _u_0 AS (
  SELECT
    CountryNotes.countrycode AS _u_1
  FROM main.CountryNotes AS CountryNotes
  JOIN main.Series AS Series
    ON CountryNotes.seriescode = Series.seriescode
    AND Series.seriescode = 'DT.DOD.DECT.CD'
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
