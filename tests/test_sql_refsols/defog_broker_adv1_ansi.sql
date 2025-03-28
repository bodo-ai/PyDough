WITH _table_alias_1 AS (
  SELECT
    SUM(sbtransaction.sbtxamount) AS agg_0,
    sbtransaction.sbtxcustid AS customer_id
  FROM main.sbtransaction AS sbtransaction
  GROUP BY
    sbtransaction.sbtxcustid
), _t0 AS (
  SELECT
    sbcustomer.sbcustname AS name,
    COALESCE(_table_alias_1.agg_0, 0) AS ordering_1,
    COALESCE(_table_alias_1.agg_0, 0) AS total_amount
  FROM main.sbcustomer AS sbcustomer
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_1.customer_id = sbcustomer.sbcustid
  ORDER BY
    ordering_1 DESC
  LIMIT 5
)
SELECT
  _t0.name AS name,
  _t0.total_amount AS total_amount
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC
