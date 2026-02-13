SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  SUBSTRING(
    LOWER(c_fname),
    2,
    CASE
      WHEN (
        LENGTH(LOWER(c_fname)) + 0
      ) < 1
      THEN 0
      ELSE CASE
        WHEN (
          (
            LENGTH(LOWER(c_fname)) + 0
          ) - 2
        ) <= 0
        THEN 0
        ELSE (
          LENGTH(LOWER(c_fname)) + 0
        ) - 2
      END
    END
  ) LIKE '%e%'
