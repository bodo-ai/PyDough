WITH months AS (
  SELECT
    CAST('1992-01-01' AS DATE) AS date_val
  UNION ALL
  SELECT
    date_val + INTERVAL '1' MONTH AS date_val
  FROM months
  WHERE
    date_val < '1998-12-01'
), _0 AS (
  SELECT
    date_val
  FROM months
), _s2 AS (
  SELECT
    date_val
  FROM _0
  ORDER BY
    1 NULLS FIRST
  LIMIT 5
), _s0 AS (
  SELECT
    date_val
  FROM _0
  ORDER BY
    1 NULLS FIRST
  LIMIT 5
), _s3 AS (
  SELECT
    _s0.date_val,
    COUNT(*) AS n_rows
  FROM _s0 AS _s0
  JOIN main.orders AS orders
    ON _s0.date_val = DATE_TRUNC('MONTH', CAST(orders.o_orderdate AS TIMESTAMP))
  GROUP BY
    1
), _s4 AS (
  SELECT
    date_val
  FROM _0
  ORDER BY
    1 NULLS FIRST
  LIMIT 5
), _s7 AS (
  SELECT
    _s4.date_val,
    COUNT(*) AS n_rows
  FROM _s4 AS _s4
  JOIN main.lineitem AS lineitem
    ON _s4.date_val = DATE_TRUNC('MONTH', CAST(lineitem.l_shipdate AS TIMESTAMP))
  GROUP BY
    1
), _s8 AS (
  SELECT
    date_val
  FROM _0
  ORDER BY
    1 NULLS FIRST
  LIMIT 5
), _s11 AS (
  SELECT
    _s8.date_val,
    COUNT(*) AS n_rows
  FROM _s8 AS _s8
  JOIN main.lineitem AS lineitem
    ON _s8.date_val = DATE_TRUNC('MONTH', CAST(lineitem.l_commitdate AS TIMESTAMP))
  GROUP BY
    1
), _s12 AS (
  SELECT
    date_val
  FROM _0
  ORDER BY
    1 NULLS FIRST
  LIMIT 5
), _s15 AS (
  SELECT
    _s12.date_val,
    COUNT(*) AS n_rows
  FROM _s12 AS _s12
  JOIN main.lineitem AS lineitem
    ON _s12.date_val = DATE_TRUNC('MONTH', CAST(lineitem.l_receiptdate AS TIMESTAMP))
  GROUP BY
    1
)
SELECT
  _s2.date_val AS month,
  COALESCE(_s3.n_rows, 0) AS orders,
  COALESCE(_s7.n_rows, 0) AS lines_shipped,
  COALESCE(_s11.n_rows, 0) AS lines_committed,
  COALESCE(_s15.n_rows, 0) AS lines_received
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.date_val = _s3.date_val
LEFT JOIN _s7 AS _s7
  ON _s2.date_val = _s7.date_val
LEFT JOIN _s11 AS _s11
  ON _s11.date_val = _s2.date_val
LEFT JOIN _s15 AS _s15
  ON _s15.date_val = _s2.date_val
ORDER BY
  1 NULLS FIRST
