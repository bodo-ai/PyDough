SELECT
  """QUOTED TABLE_NAME"""."`cast`" AS cast_,
  """QUOTED TABLE_NAME"""."`name""[" AS name,
  """QUOTED TABLE_NAME"""."= ""QUOTE""" AS quote_,
  "lowercase_detail"."0 = 0 and '" AS _0_0_and,
  "lowercase_detail"."as" AS as_
FROM KEYWORDS."""QUOTED TABLE_NAME""" """QUOTED TABLE_NAME"""
JOIN KEYWORDS."lowercase_detail" "lowercase_detail"
  ON """QUOTED TABLE_NAME"""."`name""[" = "lowercase_detail".id
WHERE
  """QUOTED TABLE_NAME"""."= ""QUOTE""" = 4
  AND """QUOTED TABLE_NAME"""."`name""[" = 7
