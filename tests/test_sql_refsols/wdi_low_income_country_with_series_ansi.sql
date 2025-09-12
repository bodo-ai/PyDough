SELECT
  country.countrycode AS country_code
FROM main.country AS country
JOIN main.countrynotes AS countrynotes
  ON country.countrycode = countrynotes.countrycode
JOIN main.series AS series
  ON series.seriescode = 'DT.DOD.DECT.CD' AND seriescode = seriescode
WHERE
  country.incomegroup = 'Low income'
