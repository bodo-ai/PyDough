SELECT
  countrycode AS country_code
FROM wdi.Country
WHERE
  incomegroup = 'Low income'
