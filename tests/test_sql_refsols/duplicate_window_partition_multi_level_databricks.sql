SELECT
  c_custkey AS key,
  c_nationkey AS nk,
  ROW_NUMBER() OVER (
    PARTITION BY c_nationkey
    ORDER BY c_acctbal DESC NULLS FIRST, c_custkey NULLS LAST
  ) AS rk
FROM tpch_s3_pq.tpch_sf1.customer
WHERE
  c_acctbal > 9999
ORDER BY
  1
