WITH _table_alias_2 AS (
  SELECT DISTINCT
    sbcustomer.sbcustcountry AS country
  FROM main.sbcustomer AS sbcustomer
), _table_alias_3 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(sbtransaction.sbtxamount) AS agg_1,
    sbcustomer.sbcustcountry AS country
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
    AND sbtransaction.sbtxdatetime >= DATE_TRUNC('DAY', DATE_ADD(CURRENT_TIMESTAMP(), -30, 'DAY'))
  GROUP BY
    sbcustomer.sbcustcountry
)
SELECT
  _table_alias_2.country AS country,
  COALESCE(_table_alias_3.agg_0, 0) AS num_transactions,
  COALESCE(_table_alias_3.agg_1, 0) AS total_amount
FROM _table_alias_2 AS _table_alias_2
LEFT JOIN _table_alias_3 AS _table_alias_3
  ON _table_alias_2.country = _table_alias_3.country
