WITH _s0 AS (
  SELECT
    COUNT(sbdpclose) AS expr_1,
    SUM(sbdpclose) AS expr_0,
    sbdptickerid AS ticker_id
  FROM main.sbdailyprice
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), sbdpdate, DAY) <= 7
  GROUP BY
    sbdptickerid
), _t0 AS (
  SELECT
    SUM(_s0.expr_0) AS expr_0,
    SUM(_s0.expr_1) AS expr_1,
    sbticker.sbtickertype AS ticker_type
  FROM _s0 AS _s0
  JOIN main.sbticker AS sbticker
    ON _s0.ticker_id = sbticker.sbtickerid
  GROUP BY
    sbticker.sbtickertype
)
SELECT
  ticker_type,
  expr_0 / expr_1 AS ACP
FROM _t0
