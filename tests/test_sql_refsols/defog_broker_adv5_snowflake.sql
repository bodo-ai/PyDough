WITH _S0 AS (
  SELECT
    SUM(sbdpclose) AS EXPR_0,
    COUNT(sbdpclose) AS EXPR_1,
    MAX(sbdphigh) AS MAX_HIGH,
    MIN(sbdplow) AS MIN_LOW,
    CONCAT_WS(
      '-',
      DATE_PART(YEAR, sbdpdate),
      CASE
        WHEN LENGTH(DATE_PART(MONTH, sbdpdate)) >= 2
        THEN SUBSTRING(DATE_PART(MONTH, sbdpdate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', DATE_PART(MONTH, sbdpdate)), (
          2 * -1
        ))
      END
    ) AS MONTH,
    sbdptickerid AS TICKER_ID
  FROM MAIN.SBDAILYPRICE
  GROUP BY
    CONCAT_WS(
      '-',
      DATE_PART(YEAR, sbdpdate),
      CASE
        WHEN LENGTH(DATE_PART(MONTH, sbdpdate)) >= 2
        THEN SUBSTRING(DATE_PART(MONTH, sbdpdate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', DATE_PART(MONTH, sbdpdate)), (
          2 * -1
        ))
      END
    ),
    sbdptickerid
), _T1 AS (
  SELECT
    SUM(_S0.EXPR_0) AS EXPR_0,
    SUM(_S0.EXPR_1) AS EXPR_1,
    MAX(_S0.MAX_HIGH) AS MAX_HIGH,
    MIN(_S0.MIN_LOW) AS MIN_LOW,
    _S0.MONTH,
    SBTICKER.sbtickersymbol AS SYMBOL
  FROM _S0 AS _S0
  JOIN MAIN.SBTICKER AS SBTICKER
    ON SBTICKER.sbtickerid = _S0.TICKER_ID
  GROUP BY
    _S0.MONTH,
    SBTICKER.sbtickersymbol
)
SELECT
  SYMBOL AS symbol,
  MONTH AS month,
  EXPR_0 / EXPR_1 AS avg_close,
  MAX_HIGH AS max_high,
  MIN_LOW AS min_low,
  (
    (
      EXPR_0 / EXPR_1
    ) - LAG((
      EXPR_0 / EXPR_1
    ), 1) OVER (PARTITION BY SYMBOL ORDER BY MONTH)
  ) / LAG((
    EXPR_0 / EXPR_1
  ), 1) OVER (PARTITION BY SYMBOL ORDER BY MONTH) AS momc
FROM _T1
