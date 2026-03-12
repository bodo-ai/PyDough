SELECT
  countrycode AS country_code
FROM main.country
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.countrynotes
    WHERE
      country.countrycode = countrycode AND seriescode = 'DT.DOD.DECT.CD'
  )
  AND incomegroup = 'Low income'
