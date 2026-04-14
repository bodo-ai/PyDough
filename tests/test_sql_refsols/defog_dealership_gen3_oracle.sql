SELECT
  payment_date,
  payment_method,
  COALESCE(SUM(payment_amount), 0) AS total_amount
FROM MAIN.PAYMENTS_RECEIVED
WHERE
  FLOOR(
    (
      TRUNC(CAST(CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) AS DATE), 'DD') - TRUNC(CAST(CAST(payment_date AS DATE) AS DATE), 'DD') + (
        MOD((
          TO_CHAR(CAST(payment_date AS DATE), 'D') + 5
        ), 7)
      ) - (
        MOD((
          TO_CHAR(CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE), 'D') + 5
        ), 7)
      )
    ) / 7
  ) = 1
GROUP BY
  payment_date,
  payment_method
ORDER BY
  1 DESC NULLS LAST,
  2 NULLS FIRST
