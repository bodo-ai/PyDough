WITH _table_alias_0 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    sbcustomer.sbcustname AS name
  FROM main.sbcustomer AS sbcustomer
), _table_alias_1 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(sbtransaction.sbtxstatus = 'success') AS agg_1,
    sbtransaction.sbtxcustid AS customer_id
  FROM main.sbtransaction AS sbtransaction
  GROUP BY
    sbtransaction.sbtxcustid
)
SELECT
  _table_alias_0.name AS name,
  (
    100.0 * COALESCE(_table_alias_1.agg_1, 0)
  ) / COALESCE(_table_alias_1.agg_0, 0) AS success_rate
FROM _table_alias_0 AS _table_alias_0
LEFT JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0._id = _table_alias_1.customer_id
WHERE
  NOT _table_alias_1.agg_0 IS NULL AND _table_alias_1.agg_0 >= 5
ORDER BY
  (
    100.0 * COALESCE(_table_alias_1.agg_1, 0)
  ) / COALESCE(_table_alias_1.agg_0, 0)
