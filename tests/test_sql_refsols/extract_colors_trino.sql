SELECT
  p_partkey AS key,
  UPPER(
    CASE
      WHEN -CAST(CAST((
        LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
      ) AS DOUBLE) / 1 AS BIGINT) > 1
      THEN NULL
      WHEN CAST(CAST((
        LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
      ) AS DOUBLE) / 1 AS BIGINT) < 1
      THEN NULL
      ELSE SPLIT_PART(p_name, ' ', 1)
    END
  ) AS c1,
  UPPER(
    CASE
      WHEN -CAST(CAST((
        LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
      ) AS DOUBLE) / 1 AS BIGINT) > 2
      THEN NULL
      WHEN CAST(CAST((
        LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
      ) AS DOUBLE) / 1 AS BIGINT) < 2
      THEN NULL
      ELSE SPLIT_PART(p_name, ' ', 2)
    END
  ) AS c2,
  UPPER(
    CASE
      WHEN -CAST(CAST((
        LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
      ) AS DOUBLE) / 1 AS BIGINT) > 3
      THEN NULL
      WHEN CAST(CAST((
        LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
      ) AS DOUBLE) / 1 AS BIGINT) < 3
      THEN NULL
      ELSE SPLIT_PART(p_name, ' ', 3)
    END
  ) AS c3,
  UPPER(
    CASE
      WHEN -CAST(CAST((
        LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
      ) AS DOUBLE) / 1 AS BIGINT) > 4
      THEN NULL
      WHEN CAST(CAST((
        LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
      ) AS DOUBLE) / 1 AS BIGINT) < 4
      THEN NULL
      ELSE SPLIT_PART(p_name, ' ', 4)
    END
  ) AS c4,
  UPPER(
    CASE
      WHEN -CAST(CAST((
        LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
      ) AS DOUBLE) / 1 AS BIGINT) > 5
      THEN NULL
      WHEN CAST(CAST((
        LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
      ) AS DOUBLE) / 1 AS BIGINT) < 5
      THEN NULL
      ELSE SPLIT_PART(p_name, ' ', 5)
    END
  ) AS c5,
  UPPER(
    CASE
      WHEN -CAST(CAST((
        LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
      ) AS DOUBLE) / 1 AS BIGINT) > 6
      THEN NULL
      WHEN CAST(CAST((
        LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
      ) AS DOUBLE) / 1 AS BIGINT) < 6
      THEN NULL
      ELSE SPLIT_PART(p_name, ' ', 6)
    END
  ) AS c6
FROM tpch.part
ORDER BY
  1 NULLS FIRST
LIMIT 5
