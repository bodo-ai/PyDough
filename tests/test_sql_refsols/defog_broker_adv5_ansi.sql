WITH _s1 AS (
  SELECT
    sbtickerid AS _id,
    sbtickersymbol AS symbol
  FROM main.sbticker
), _s4 AS (
  SELECT DISTINCT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM sbdailyprice.sbdpdate),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM sbdailyprice.sbdpdate)) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM sbdailyprice.sbdpdate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM sbdailyprice.sbdpdate)), -2)
      END
    ) AS month,
    _s1.symbol
  FROM main.sbdailyprice AS sbdailyprice
  JOIN _s1 AS _s1
    ON _s1._id = sbdailyprice.sbdptickerid
), _s2 AS (
  SELECT
    COUNT(sbdpclose) AS expr_1,
    MAX(sbdphigh) AS agg_1,
    MIN(sbdplow) AS agg_2,
    SUM(sbdpclose) AS expr_0,
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM sbdpdate),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM sbdpdate)) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM sbdpdate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM sbdpdate)), (
          2 * -1
        ))
      END
    ) AS month,
    sbdptickerid AS ticker_id
  FROM main.sbdailyprice
  GROUP BY
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM sbdpdate),
      CASE
        WHEN LENGTH(EXTRACT(MONTH FROM sbdpdate)) >= 2
        THEN SUBSTRING(EXTRACT(MONTH FROM sbdpdate), 1, 2)
        ELSE SUBSTRING(CONCAT('00', EXTRACT(MONTH FROM sbdpdate)), (
          2 * -1
        ))
      END
    ),
    sbdptickerid
), _t2 AS (
  SELECT
    MAX(_s2.agg_1) AS agg_1,
    MIN(_s2.agg_2) AS agg_2,
    SUM(_s2.expr_0) AS expr_0,
    SUM(_s2.expr_1) AS expr_1,
    _s2.month,
    _s3.symbol
  FROM _s2 AS _s2
  JOIN _s1 AS _s3
    ON _s2.ticker_id = _s3._id
  GROUP BY
    _s2.month,
    _s3.symbol
)
SELECT
  _s4.symbol,
  _s4.month,
  _t2.expr_0 / _t2.expr_1 AS avg_close,
  _t2.agg_1 AS max_high,
  _t2.agg_2 AS min_low,
  (
    (
      _t2.expr_0 / _t2.expr_1
    ) - LAG((
      _t2.expr_0 / _t2.expr_1
    ), 1) OVER (PARTITION BY _s4.symbol ORDER BY _s4.month NULLS LAST)
  ) / LAG((
    _t2.expr_0 / _t2.expr_1
  ), 1) OVER (PARTITION BY _s4.symbol ORDER BY _s4.month NULLS LAST) AS momc
FROM _s4 AS _s4
JOIN _t2 AS _t2
  ON _s4.month = _t2.month AND _s4.symbol = _t2.symbol
