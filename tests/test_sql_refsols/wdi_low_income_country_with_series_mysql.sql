SELECT
  countrycode AS country_code
FROM main.Country
WHERE
  incomegroup = 'Low income'
  AND EXISTS(
    SELECT
      1 AS `1`
    FROM main.CountryNotes
    WHERE
      Country.countrycode = countrycode AND seriescode = 'DT.DOD.DECT.CD'
  )
