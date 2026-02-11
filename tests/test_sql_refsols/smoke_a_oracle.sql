SELECT
  p_partkey AS key,
  CAST(LISTAGG(
    '',
    SUBSTR(p_brand, CASE WHEN ABS(-2) < LENGTH(p_brand) THEN -2 ELSE 1 END),
    SUBSTR(p_brand, 8),
    SUBSTR(
      p_brand,
      CASE WHEN ABS(-2) < LENGTH(p_brand) THEN -2 ELSE 1 END,
      CASE
        WHEN ABS(-2) > LENGTH(p_brand)
        THEN LENGTH(p_brand) + -1
        ELSE GREATEST(1, 0)
      END
    )
  ) AS INT) AS a,
  UPPER(
    LEAST(
      REGEXP_SUBSTR(p_name, '[^ ]+', 1, 2),
      REGEXP_SUBSTR(p_name, '[^ ]+', 1, REGEXP_COUNT(p_name, ' ') + 1)
    )
  ) AS b,
  TRIM('o' FROM SUBSTR(p_name, 1, 2)) AS c,
  CASE
    WHEN LENGTH(CAST(p_size AS CLOB)) >= 3
    THEN SUBSTR(CAST(p_size AS CLOB), 1, 3)
    ELSE SUBSTR(CONCAT('000', CAST(p_size AS CLOB)), -3)
  END AS d,
  SUBSTR(CONCAT(CAST(p_size AS CLOB), '000'), 1, 3) AS e,
  REPLACE(p_mfgr, 'Manufacturer#', 'm') AS f,
  REPLACE(LOWER(p_container), ' ', '') AS g,
  CASE
    WHEN LENGTH('o') = 0
    THEN 0
    ELSE CAST((
      LENGTH(p_name) - LENGTH(REPLACE(p_name, 'o', ''))
    ) / LENGTH('o') AS INT)
  END + (
    INSTR(p_name, 'o') / 100.0
  ) AS h,
  ROUND(POWER(GREATEST(p_size, 10), 0.5), 3) AS i
FROM TPCH.PART
ORDER BY
  1 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
