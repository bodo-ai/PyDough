SELECT
  country.countrycode AS country_code
FROM main.country AS country
JOIN main.countrynotes AS countrynotes
  ON country.countrycode = countrynotes.countrycode
  AND countrynotes.seriescode = 'DT.DOD.DECT.CD'
WHERE
  country.incomegroup = 'Low income'
