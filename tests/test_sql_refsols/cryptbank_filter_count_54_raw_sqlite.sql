SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  SUBSTRING(
    LOWER(c_fname),
    CASE
      WHEN (
        LENGTH(LOWER(c_fname)) + 0
      ) < 1
      THEN 1
      ELSE (
        LENGTH(LOWER(c_fname)) + 0
      )
    END
  ) IN ('a', 'e', 'i', 'o', 'u')
