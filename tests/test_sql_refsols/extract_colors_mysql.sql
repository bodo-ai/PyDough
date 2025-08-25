SELECT
  p_partkey AS `key`,
  UPPER(
    CASE
      WHEN CHAR_LENGTH(p_name) = 0
      THEN NULL
      WHEN CHAR_LENGTH(' ') = 0
      THEN CASE WHEN ABS(1) = 1 THEN p_name ELSE NULL END
      WHEN (
        CHAR_LENGTH(p_name) - CHAR_LENGTH(REPLACE(p_name, ' ', ''))
      ) / CHAR_LENGTH(' ') >= 0
      THEN SUBSTRING_INDEX(SUBSTRING_INDEX(p_name, ' ', 1), ' ', -1)
      ELSE NULL
    END
  ) AS c1,
  UPPER(
    CASE
      WHEN CHAR_LENGTH(p_name) = 0
      THEN NULL
      WHEN CHAR_LENGTH(' ') = 0
      THEN CASE WHEN ABS(2) = 1 THEN p_name ELSE NULL END
      WHEN (
        CHAR_LENGTH(p_name) - CHAR_LENGTH(REPLACE(p_name, ' ', ''))
      ) / CHAR_LENGTH(' ') >= 1
      THEN SUBSTRING_INDEX(SUBSTRING_INDEX(p_name, ' ', 2), ' ', -1)
      ELSE NULL
    END
  ) AS c2,
  UPPER(
    CASE
      WHEN CHAR_LENGTH(p_name) = 0
      THEN NULL
      WHEN CHAR_LENGTH(' ') = 0
      THEN CASE WHEN ABS(3) = 1 THEN p_name ELSE NULL END
      WHEN (
        CHAR_LENGTH(p_name) - CHAR_LENGTH(REPLACE(p_name, ' ', ''))
      ) / CHAR_LENGTH(' ') >= 2
      THEN SUBSTRING_INDEX(SUBSTRING_INDEX(p_name, ' ', 3), ' ', -1)
      ELSE NULL
    END
  ) AS c3,
  UPPER(
    CASE
      WHEN CHAR_LENGTH(p_name) = 0
      THEN NULL
      WHEN CHAR_LENGTH(' ') = 0
      THEN CASE WHEN ABS(4) = 1 THEN p_name ELSE NULL END
      WHEN (
        CHAR_LENGTH(p_name) - CHAR_LENGTH(REPLACE(p_name, ' ', ''))
      ) / CHAR_LENGTH(' ') >= 3
      THEN SUBSTRING_INDEX(SUBSTRING_INDEX(p_name, ' ', 4), ' ', -1)
      ELSE NULL
    END
  ) AS c4,
  UPPER(
    CASE
      WHEN CHAR_LENGTH(p_name) = 0
      THEN NULL
      WHEN CHAR_LENGTH(' ') = 0
      THEN CASE WHEN ABS(5) = 1 THEN p_name ELSE NULL END
      WHEN (
        CHAR_LENGTH(p_name) - CHAR_LENGTH(REPLACE(p_name, ' ', ''))
      ) / CHAR_LENGTH(' ') >= 4
      THEN SUBSTRING_INDEX(SUBSTRING_INDEX(p_name, ' ', 5), ' ', -1)
      ELSE NULL
    END
  ) AS c5,
  UPPER(
    CASE
      WHEN CHAR_LENGTH(p_name) = 0
      THEN NULL
      WHEN CHAR_LENGTH(' ') = 0
      THEN CASE WHEN ABS(6) = 1 THEN p_name ELSE NULL END
      WHEN (
        CHAR_LENGTH(p_name) - CHAR_LENGTH(REPLACE(p_name, ' ', ''))
      ) / CHAR_LENGTH(' ') >= 5
      THEN SUBSTRING_INDEX(SUBSTRING_INDEX(p_name, ' ', 6), ' ', -1)
      ELSE NULL
    END
  ) AS c6
FROM tpch.part
ORDER BY
  1
LIMIT 5
