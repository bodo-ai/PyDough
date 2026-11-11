SELECT
  REPLACE(CAST(c_custkey AS TEXT), '1', 'one') AS clean_key
FROM main.customer
WHERE
  c_custkey <= 5
ORDER BY
  c_custkey NULLS FIRST
