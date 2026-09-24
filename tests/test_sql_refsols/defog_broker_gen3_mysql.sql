WITH _s1 AS (
  SELECT
    sbTxCustId,
    MIN(sbTxDateTime) AS min_sbTxDateTime
  FROM broker.sbTransaction
  GROUP BY
    1
)
SELECT
  sbCustomer.sbCustId AS cust_id,
  TIMESTAMPDIFF(SECOND, sbCustomer.sbCustJoinDate, _s1.min_sbTxDateTime) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM broker.sbCustomer AS sbCustomer
JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbCustId
