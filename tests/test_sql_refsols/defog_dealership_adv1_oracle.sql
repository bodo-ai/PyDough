SELECT
  TRUNC(CAST(PAYMENTS_RECEIVED.payment_date AS TIMESTAMP), 'WEEK') AS payment_week,
  COUNT(*) AS total_payments,
  COALESCE(
    SUM(
      (
        MOD((
          TO_CHAR(PAYMENTS_RECEIVED.payment_date, 'D') + 5
        ), 7)
      ) IN (5, 6)
    ),
    0
  ) AS weekend_payments
FROM MAIN.PAYMENTS_RECEIVED PAYMENTS_RECEIVED
JOIN MAIN.SALES SALES
  ON PAYMENTS_RECEIVED.sale_id = SALES."_id" AND SALES.sale_price > 30000
WHERE
  FLOOR(
    (
      CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) - CAST(PAYMENTS_RECEIVED.payment_date AS DATE) + (
        MOD((
          TO_CHAR(PAYMENTS_RECEIVED.payment_date, 'D') + 5
        ), 7)
      ) - (
        MOD((
          TO_CHAR('now', 'D') + 5
        ), 7)
      )
    ) / 7
  ) <= 8
  AND FLOOR(
    (
      CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) - CAST(PAYMENTS_RECEIVED.payment_date AS DATE) + (
        MOD((
          TO_CHAR(PAYMENTS_RECEIVED.payment_date, 'D') + 5
        ), 7)
      ) - (
        MOD((
          TO_CHAR('now', 'D') + 5
        ), 7)
      )
    ) / 7
  ) >= 1
GROUP BY
  TRUNC(CAST(PAYMENTS_RECEIVED.payment_date AS TIMESTAMP), 'WEEK')
