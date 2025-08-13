SELECT
  p_partkey AS key,
  UPPER(SPLIT_PART(p_name, ' ', 1)) AS c1,
  UPPER(SPLIT_PART(p_name, ' ', 2)) AS c2,
  UPPER(SPLIT_PART(p_name, ' ', 3)) AS c3,
  UPPER(SPLIT_PART(p_name, ' ', 4)) AS c4,
  UPPER(SPLIT_PART(p_name, ' ', 5)) AS c5,
  UPPER(SPLIT_PART(p_name, ' ', 6)) AS c6
FROM TPCH.PART
ORDER BY
  p_partkey NULLS FIRST
LIMIT 5
