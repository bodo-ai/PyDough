SELECT
  SUBSTRING(word, 1, 1) AS first_letter,
  MEDIAN(ccount) AS median_length
FROM dict
WHERE
  SUBSTRING(word, 1, 1) IN ('a', 'e', 'i', 'o', 'u', 'y') AND pos = 'n.'
GROUP BY
  1
