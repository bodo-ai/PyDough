WITH _s1 AS (
  SELECT
    SUM(sbdpclose) AS expr_0,
    COUNT(sbdpclose) AS expr_1,
    sbdptickerid AS ticker_id
  FROM main.sbdailyprice
  WHERE
    CAST((
      JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(sbdpdate, 'start of day'))
    ) AS INTEGER) <= 7
  GROUP BY
    sbdptickerid
), _t0 AS (
  SELECT
    SUM(_s1.expr_0) AS expr_0,
    SUM(_s1.expr_1) AS expr_1,
    sbticker.sbtickertype AS ticker_type
  FROM main.sbticker AS sbticker
  JOIN _s1 AS _s1
    ON _s1.ticker_id = sbticker.sbtickerid
  GROUP BY
    sbticker.sbtickertype
)
SELECT
  ticker_type,
  CAST(expr_0 AS REAL) / expr_1 AS ACP
FROM _t0
