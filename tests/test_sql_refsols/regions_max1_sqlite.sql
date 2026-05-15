SELECT
  r_regionkey AS key,
  r_name AS name,
  r_comment AS comment
FROM tpch.region
LIMIT 1
