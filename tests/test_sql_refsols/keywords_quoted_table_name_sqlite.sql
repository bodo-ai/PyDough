SELECT
  """quoted table_name"""."`cast`" AS cast_,
  """quoted table_name"""."`name""[" AS name,
  """quoted table_name"""."= ""quote""" AS quote_,
  lowercase_detail."0 = 0 and '" AS _0_0_and,
  lowercase_detail."as" AS as_
FROM keywords."""quoted table_name""" AS """quoted table_name"""
JOIN keywords.lowercase_detail AS lowercase_detail
  ON """quoted table_name"""."`name""[" = lowercase_detail.id
WHERE
  """quoted table_name"""."= ""quote""" = 4
  AND """quoted table_name"""."`name""[" = 7
