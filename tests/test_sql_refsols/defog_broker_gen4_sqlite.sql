WITH _table_alias_0 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    sbcustomer.sbcustname AS name
  FROM main.sbcustomer AS sbcustomer
), _table_alias_1 AS (
  SELECT
    COUNT() AS agg_0,
    sbtransaction.sbtxcustid AS customer_id
  FROM main.sbtransaction AS sbtransaction
  WHERE
    DATE(sbtransaction.sbtxdatetime, 'start of day') = DATE('2023-04-01')
    AND sbtransaction.sbtxtype = 'sell'
  GROUP BY
    sbtransaction.sbtxcustid
), _t0 AS (
  SELECT
    _table_alias_0._id AS _id,
    _table_alias_0.name AS name,
    COALESCE(_table_alias_1.agg_0, 0) AS num_tx,
    COALESCE(_table_alias_1.agg_0, 0) AS ordering_1
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0._id = _table_alias_1.customer_id
  ORDER BY
    ordering_1 DESC
  LIMIT 1
)
SELECT
  _t0._id AS _id,
  _t0.name AS name,
  _t0.num_tx AS num_tx
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC
