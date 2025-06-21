WITH _S1 AS (
  SELECT
    MAX(payment_date) AS AGG_0,
    sale_id AS SALE_ID
  FROM MAIN.PAYMENTS_RECEIVED
  GROUP BY
    sale_id
), _T0 AS (
  SELECT
    AVG(DATEDIFF(DAY, SALES.sale_date, _S1.AGG_0)) AS AGG_0
  FROM MAIN.SALES AS SALES
  LEFT JOIN _S1 AS _S1
    ON SALES._id = _S1.SALE_ID
)
SELECT
  ROUND(AGG_0, 2) AS avg_days_to_payment
FROM _T0
