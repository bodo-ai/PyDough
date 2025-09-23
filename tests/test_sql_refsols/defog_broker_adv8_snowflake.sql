SELECT
  CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE NULL END AS n_transactions,
  COALESCE(SUM(sbtxamount), 0) AS total_amount
FROM main.sbtransaction
WHERE
  sbtxdatetime < DATE_TRUNC(
    'DAY',
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 6
        ) % 7
      ) * -1,
      CURRENT_TIMESTAMP()
    )
  )
  AND sbtxdatetime >= DATEADD(
    WEEK,
    -1,
    DATE_TRUNC(
      'DAY',
      DATEADD(
        DAY,
        (
          (
            DAYOFWEEK(CURRENT_TIMESTAMP()) + 6
          ) % 7
        ) * -1,
        CURRENT_TIMESTAMP()
      )
    )
  )
