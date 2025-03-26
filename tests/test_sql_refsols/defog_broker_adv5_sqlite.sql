WITH _table_alias_0 AS (
  SELECT
    sbdailyprice.sbdpdate AS date,
    sbdailyprice.sbdptickerid AS ticker_id
  FROM main.sbdailyprice AS sbdailyprice
), _table_alias_1 AS (
  SELECT
    sbticker.sbtickerid AS _id,
    sbticker.sbtickersymbol AS symbol
  FROM main.sbticker AS sbticker
), _table_alias_4 AS (
  SELECT DISTINCT
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', _table_alias_0.date) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', _table_alias_0.date) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', _table_alias_0.date) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', _table_alias_0.date) AS INTEGER), -2)
      END
    ) AS month,
    _table_alias_1.symbol AS symbol
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.ticker_id = _table_alias_1._id
), _table_alias_2 AS (
  SELECT
    sbdailyprice.sbdpclose AS close,
    sbdailyprice.sbdpdate AS date,
    sbdailyprice.sbdphigh AS high,
    sbdailyprice.sbdplow AS low,
    sbdailyprice.sbdptickerid AS ticker_id
  FROM main.sbdailyprice AS sbdailyprice
), _table_alias_5 AS (
  SELECT
    AVG(_table_alias_2.close) AS agg_0,
    MAX(_table_alias_2.high) AS agg_1,
    MIN(_table_alias_2.low) AS agg_2,
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', _table_alias_2.date) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', _table_alias_2.date) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', _table_alias_2.date) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', _table_alias_2.date) AS INTEGER), (
          2 * -1
        ))
      END
    ) AS month,
    _table_alias_3.symbol AS symbol
  FROM _table_alias_2 AS _table_alias_2
  LEFT JOIN _table_alias_1 AS _table_alias_3
    ON _table_alias_2.ticker_id = _table_alias_3._id
  GROUP BY
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', _table_alias_2.date) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', _table_alias_2.date) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', _table_alias_2.date) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', _table_alias_2.date) AS INTEGER), (
          2 * -1
        ))
      END
    ),
    _table_alias_3.symbol
)
SELECT
  _table_alias_4.symbol AS symbol,
  _table_alias_4.month AS month,
  _table_alias_5.agg_0 AS avg_close,
  _table_alias_5.agg_1 AS max_high,
  _table_alias_5.agg_2 AS min_low,
  CAST((
    _table_alias_5.agg_0 - LAG(_table_alias_5.agg_0, 1) OVER (PARTITION BY _table_alias_4.symbol ORDER BY _table_alias_4.month)
  ) AS REAL) / LAG(_table_alias_5.agg_0, 1) OVER (PARTITION BY _table_alias_4.symbol ORDER BY _table_alias_4.month) AS momc
FROM _table_alias_4 AS _table_alias_4
LEFT JOIN _table_alias_5 AS _table_alias_5
  ON _table_alias_4.month = _table_alias_5.month
  AND _table_alias_4.symbol = _table_alias_5.symbol
