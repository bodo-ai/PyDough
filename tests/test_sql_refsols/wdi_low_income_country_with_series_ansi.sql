SELECT
  country.countrycode AS country_code
FROM wdi.country AS country
JOIN wdi.countrynotes AS countrynotes
  ON country.countrycode = countrynotes.countrycode
JOIN wdi.series AS series
  ON countrynotes.seriescode = series.seriescode
  AND series.seriescode = 'DT.DOD.DECT.CD'
WHERE
  country.incomegroup = 'Low income'
