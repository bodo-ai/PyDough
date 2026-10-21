SELECT
  """," AS "dbl_quote_dot",
  "." AS "dot",
  "." + COALESCE("FLOAT", STR, 1) AS addition,
  "__col__" AS "col",
  "__col1__" AS "col1",
  DEF AS def_,
  DEL AS "__del__",
  "__init__"
FROM KEYWORDS."COUNT"
WHERE
  "int" = 8051
