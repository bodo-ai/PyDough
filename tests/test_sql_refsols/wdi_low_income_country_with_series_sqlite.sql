SELECT
  countrycode AS country_code
FROM wdi.country
WHERE
  incomegroup = 'Low income'
