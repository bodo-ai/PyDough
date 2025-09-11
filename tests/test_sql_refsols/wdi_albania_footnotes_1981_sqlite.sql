SELECT
  footnotes.description AS footnote_description
FROM main.country AS country
JOIN main.footnotes AS footnotes
  ON countrycode = countrycode AND footnotes.year = 'YR1978'
WHERE
  country.shortname = 'Albania'
