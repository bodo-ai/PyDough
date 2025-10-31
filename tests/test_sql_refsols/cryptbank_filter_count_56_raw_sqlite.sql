SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  SUBSTRING(
    LOWER(c_fname),
    CASE
      WHEN (
        LENGTH(LOWER(c_fname)) + -1
      ) < 1
      THEN 1
      ELSE (
        LENGTH(LOWER(c_fname)) + -1
      )
    END,
    CASE
      WHEN (
        LENGTH(LOWER(c_fname)) + 0
      ) < 1
      THEN 0
      ELSE (
        LENGTH(LOWER(c_fname)) + 0
      ) - CASE
        WHEN (
          LENGTH(LOWER(c_fname)) + -1
        ) < 1
        THEN 1
        ELSE (
          LENGTH(LOWER(c_fname)) + -1
        )
      END
    END
  ) IN ('a', 'c', 'l')
