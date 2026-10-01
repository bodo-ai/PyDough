SELECT
  Footnotes.Description AS footnote_description
FROM main.Country AS Country
JOIN main.Footnotes AS Footnotes
  ON Country.CountryCode = Footnotes.Countrycode AND Footnotes.Year = 'YR2012'
WHERE
  Country.ShortName = 'Albania'
