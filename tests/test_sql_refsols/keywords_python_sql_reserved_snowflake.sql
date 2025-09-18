SELECT
  (
    (
      `.`
    ).`
  ).` AS dot,
  (
    (
      `.`
    ).`
  ).` + COALESCE(float, str, 1) AS addition,
  __col__ AS col,
  __col1__ AS col1,
  def AS def_,
  del AS __del__,
  __init__
FROM keywords.count
WHERE
  int = 8051
