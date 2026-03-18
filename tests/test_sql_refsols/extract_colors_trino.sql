SELECT
  p_partkey AS key,
  UPPER(
    CASE
      WHEN -CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END > 1
      THEN NULL
      WHEN CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END < 1
      THEN NULL
      ELSE SPLIT_PART(p_name, ' ', 1)
    END
  ) AS c1,
  UPPER(
    CASE
      WHEN -CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END > 2
      THEN NULL
      WHEN CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END < 2
      THEN NULL
      ELSE SPLIT_PART(p_name, ' ', 2)
    END
  ) AS c2,
  UPPER(
    CASE
      WHEN -CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END > 3
      THEN NULL
      WHEN CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END < 3
      THEN NULL
      ELSE SPLIT_PART(p_name, ' ', 3)
    END
  ) AS c3,
  UPPER(
    CASE
      WHEN -CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END > 4
      THEN NULL
      WHEN CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END < 4
      THEN NULL
      ELSE SPLIT_PART(p_name, ' ', 4)
    END
  ) AS c4,
  UPPER(
    CASE
      WHEN -CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END > 5
      THEN NULL
      WHEN CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END < 5
      THEN NULL
      ELSE SPLIT_PART(p_name, ' ', 5)
    END
  ) AS c5,
  UPPER(
    CASE
      WHEN -CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END > 6
      THEN NULL
      WHEN CASE
        WHEN LENGTH(' ') = 0
        THEN 0
        ELSE CAST(CAST((
          LENGTH(p_name) - LENGTH(REPLACE(p_name, ' ', ''))
        ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
      END < 6
      THEN NULL
      ELSE SPLIT_PART(p_name, ' ', 6)
    END
  ) AS c6
FROM tpch.part
ORDER BY
  1 NULLS FIRST
LIMIT 5
