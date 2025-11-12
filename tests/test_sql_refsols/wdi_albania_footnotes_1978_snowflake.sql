SELECT
  footnotes.description AS footnote_description
FROM wdi.country AS country
JOIN wdi.footnotes AS footnotes
  ON country.countrycode = footnotes.countrycode AND footnotes."Year" = 'YR2012'
WHERE
  country.shortname = 'Albania'
