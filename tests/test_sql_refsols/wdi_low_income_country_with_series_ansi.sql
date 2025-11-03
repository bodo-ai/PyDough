SELECT
  country.countrycode AS country_code
FROM wdi.country AS country
JOIN wdi.countrynotes AS countrynotes
  ON country.countrycode = countrynotes.countrycode
  AND countrynotes.seriescode = 'DT.DOD.DECT.CD'
WHERE
  country.incomegroup = 'Low income'
