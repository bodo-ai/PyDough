WITH _table_alias_1 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(sbtransaction.sbtxstatus = 'success') AS agg_1,
    sbtransaction.sbtxcustid AS customer_id
  FROM main.sbtransaction AS sbtransaction
  GROUP BY
    sbtransaction.sbtxcustid
)
SELECT
  sbcustomer.sbcustname AS name,
  CAST((
    100.0 * COALESCE(_table_alias_1.agg_1, 0)
  ) AS REAL) / COALESCE(_table_alias_1.agg_0, 0) AS success_rate
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_1.customer_id = sbcustomer.sbcustid
WHERE
  NOT _table_alias_1.agg_0 IS NULL AND _table_alias_1.agg_0 >= 5
ORDER BY
  CAST((
    100.0 * COALESCE(_table_alias_1.agg_1, 0)
  ) AS REAL) / COALESCE(_table_alias_1.agg_0, 0)
