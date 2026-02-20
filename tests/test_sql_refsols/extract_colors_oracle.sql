SELECT
  p_partkey AS key,
  UPPER(REGEXP_SUBSTR(p_name, '[^ ]+', 1, 1)) AS c1,
  UPPER(REGEXP_SUBSTR(p_name, '[^ ]+', 1, 2)) AS c2,
  UPPER(REGEXP_SUBSTR(p_name, '[^ ]+', 1, 3)) AS c3,
  UPPER(REGEXP_SUBSTR(p_name, '[^ ]+', 1, 4)) AS c4,
  UPPER(REGEXP_SUBSTR(p_name, '[^ ]+', 1, 5)) AS c5,
  UPPER(REGEXP_SUBSTR(p_name, '[^ ]+', 1, 6)) AS c6
FROM TPCH.PART
ORDER BY
  1 NULLS FIRST
FETCH FIRST 5 ROWS ONLY
