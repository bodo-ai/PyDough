SELECT
  countrycode AS country_code
FROM main.country
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.countrynotes AS countrynotes
    JOIN main.series AS series
      ON series.seriescode = 'DT.DOD.DECT.CD' AND seriescode = seriescode
    WHERE
      country.countrycode = countrynotes.countrycode
  )
  AND incomegroup = 'Low income'
