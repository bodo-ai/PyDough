SELECT
  footnotes.description AS footnote_description
FROM main.country AS country
JOIN main.footnotes AS footnotes
  ON country.countrycode = footnotes.countrycode AND footnotes.year = 'YR2012'
WHERE
  country.shortname = 'Albania'
