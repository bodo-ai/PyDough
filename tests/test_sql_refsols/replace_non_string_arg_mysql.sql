SELECT
  REPLACE(c_custkey, '1', 'one') AS clean_key
FROM tpch.CUSTOMER
WHERE
  c_custkey <= 5
ORDER BY
  c_custkey
