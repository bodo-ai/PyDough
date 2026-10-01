SELECT
  REPLACE(c_custkey, '1', 'one') AS clean_key
FROM tpch.customer
WHERE
  c_custkey <= 5
ORDER BY
  c_custkey NULLS FIRST
