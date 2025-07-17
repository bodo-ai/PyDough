WITH _s3 AS (
  SELECT
    COALESCE(SUM(l_quantity), 0) AS agg_0,
    l_partkey
  FROM tpch.LINEITEM
  WHERE
    YEAR(l_shipdate) = 1994
  GROUP BY
    l_partkey
), _s5 AS (
  SELECT
    _s3.agg_0,
    PART.p_partkey
  FROM tpch.PART AS PART
  JOIN _s3 AS _s3
    ON PART.p_partkey = _s3.l_partkey
  WHERE
    PART.p_name LIKE 'forest%'
), _t1 AS (
  SELECT
    COUNT(*) AS n_rows,
    PARTSUPP.ps_suppkey
  FROM tpch.PARTSUPP AS PARTSUPP
  JOIN _s5 AS _s5
    ON PARTSUPP.ps_availqty > (
      0.5 * COALESCE(_s5.agg_0, 0)
    )
    AND PARTSUPP.ps_partkey = _s5.p_partkey
  GROUP BY
    PARTSUPP.ps_suppkey
)
SELECT
  SUPPLIER.s_name AS S_NAME,
  SUPPLIER.s_address AS S_ADDRESS
FROM tpch.SUPPLIER AS SUPPLIER
JOIN tpch.NATION AS NATION
  ON NATION.n_name = 'CANADA' AND NATION.n_nationkey = SUPPLIER.s_nationkey
JOIN _t1 AS _t1
  ON SUPPLIER.s_suppkey = _t1.ps_suppkey AND _t1.n_rows > 0
ORDER BY
  SUPPLIER.s_name
LIMIT 10
