SELECT
  TRUNC(
    CAST(CAST(SBTRANSACTION.sbtxdatetime AS DATE) - MOD((
      TO_CHAR(CAST(SBTRANSACTION.sbtxdatetime AS DATE), 'D') + 5
    ), 7) AS DATE),
    'DD'
  ) AS week,
  COUNT(*) AS num_transactions,
  COALESCE(
    SUM((
      MOD((
        TO_CHAR(SBTRANSACTION.sbtxdatetime, 'D') + 5
      ), 7)
    ) IN (5, 6)),
    0
  ) AS weekend_transactions
FROM MAIN.SBTRANSACTION SBTRANSACTION
JOIN MAIN.SBTICKER SBTICKER
  ON SBTICKER.sbtickerid = SBTRANSACTION.sbtxtickerid
  AND SBTICKER.sbtickertype = 'stock'
WHERE
  SBTRANSACTION.sbtxdatetime < TRUNC(
    SYS_EXTRACT_UTC(SYSTIMESTAMP) - MOD((
      TO_CHAR(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'D') + 5
    ), 7),
    'DD'
  )
  AND SBTRANSACTION.sbtxdatetime >= (
    TRUNC(
      SYS_EXTRACT_UTC(SYSTIMESTAMP) - MOD((
        TO_CHAR(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'D') + 5
      ), 7),
      'DD'
    ) - NUMTODSINTERVAL(56, 'DAY')
  )
GROUP BY
  TRUNC(
    CAST(CAST(SBTRANSACTION.sbtxdatetime AS DATE) - MOD((
      TO_CHAR(CAST(SBTRANSACTION.sbtxdatetime AS DATE), 'D') + 5
    ), 7) AS DATE),
    'DD'
  )
