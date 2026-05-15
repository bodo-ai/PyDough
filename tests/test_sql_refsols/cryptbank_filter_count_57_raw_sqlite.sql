SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  SUBSTRING(
    LOWER(c_fname),
    1,
    CASE
      WHEN (
        LENGTH(LOWER(c_fname)) + -1
      ) < 0
      THEN 0
      ELSE (
        LENGTH(LOWER(c_fname)) + -1
      )
    END
  ) LIKE '%e%'
