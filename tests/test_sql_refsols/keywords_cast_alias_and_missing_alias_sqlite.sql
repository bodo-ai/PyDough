SELECT
  cast.id2 AS id1,
  cast.id AS id2,
  lowercase_detail_2."two words" AS fk2_two_words
FROM keywords."cast" AS cast
JOIN keywords.lowercase_detail AS lowercase_detail
  ON cast.id2 = lowercase_detail.id
  AND lowercase_detail."0 = 0 and '" = '2 "0 = 0 and ''" field name'
JOIN keywords.lowercase_detail AS lowercase_detail_2
  ON cast.id = lowercase_detail_2.id AND lowercase_detail_2.id = 1
