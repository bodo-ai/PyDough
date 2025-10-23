SELECT
  `",` AS dbl_quote_dot,
  `.` AS dot,
  `.` + COALESCE(`FLOAT`, str, 1) AS addition,
  `__col__` AS col,
  `__col1__` AS col1,
  def AS def_,
  del AS __del__,
  `__init__`
FROM keywords.`COUNT`
WHERE
  `int` = 8051
