WITH _s3 AS (
  SELECT
    l_partkey,
    SUM(l_quantity) AS sum_l_quantity
  FROM tpch.LINEITEM
  WHERE
    EXTRACT(YEAR FROM CAST(l_shipdate AS DATETIME)) = 1994
  GROUP BY
    1
), _s5 AS (
  SELECT
    PART.p_partkey,
    _s3.sum_l_quantity
  FROM tpch.PART AS PART
  JOIN _s3 AS _s3
    ON PART.p_partkey = _s3.l_partkey
  WHERE
    PART.p_name LIKE 'forest%'
), _s7 AS (
  SELECT DISTINCT
    PARTSUPP.ps_suppkey
  FROM tpch.PARTSUPP AS PARTSUPP
  JOIN _s5 AS _s5
    ON PARTSUPP.ps_availqty > (
      0.5 * COALESCE(_s5.sum_l_quantity, 0)
    )
    AND PARTSUPP.ps_partkey = _s5.p_partkey
)
SELECT
  SUPPLIER.s_name COLLATE utf8mb4_bin AS S_NAME,
  SUPPLIER.s_address AS S_ADDRESS
FROM tpch.SUPPLIER AS SUPPLIER
JOIN tpch.NATION AS NATION
  ON NATION.n_name = 'CANADA' AND NATION.n_nationkey = SUPPLIER.s_nationkey
JOIN _s7 AS _s7
  ON SUPPLIER.s_suppkey = _s7.ps_suppkey
ORDER BY
  1
LIMIT 10
