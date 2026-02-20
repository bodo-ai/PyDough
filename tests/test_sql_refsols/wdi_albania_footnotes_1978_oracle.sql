SELECT
  FOOTNOTES.description AS footnote_description
FROM MAIN.COUNTRY COUNTRY
JOIN MAIN.FOOTNOTES FOOTNOTES
  ON COUNTRY.countrycode = FOOTNOTES.countrycode AND FOOTNOTES.year = 'YR2012'
WHERE
  COUNTRY.shortname = 'Albania'
