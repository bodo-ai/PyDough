SELECT
  Footnotes.description AS footnote_description
FROM wdi.Country AS Country
JOIN wdi.Footnotes AS Footnotes
  ON Country.countrycode = Footnotes.countrycode AND Footnotes.`Year` = 'YR2012'
WHERE
  Country.shortname = 'Albania'
