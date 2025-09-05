SELECT
  Footnotes.description AS footnote_description
FROM main.Country AS Country
JOIN main.Footnotes AS Footnotes
  ON Country.countrycode = Footnotes.countrycode AND Footnotes.year = '1981'
WHERE
  Country.shortname = 'Albania'
