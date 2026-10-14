SELECT
  c_nationkey AS `key`,
  c_nationkey AS key_again,
  COUNT(*) AS cnt
FROM tpch.CUSTOMER
GROUP BY
  1
ORDER BY
  1
