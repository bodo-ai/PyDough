SELECT
  country.countrycode AS country_code
FROM main.country AS country
JOIN main.countrynotes AS countrynotes
  ON country.countrycode = countrynotes.countrycode
JOIN main.series AS series
  ON countrynotes.seriescode = series.seriescode
  AND series.seriescode = 'DT.DOD.DECT.CD'
WHERE
  country.incomegroup = 'Low income'
