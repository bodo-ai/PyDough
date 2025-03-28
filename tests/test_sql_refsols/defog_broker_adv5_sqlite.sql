WITH _table_alias_1 AS (
  SELECT
    sbticker.sbtickerid AS _id,
    sbticker.sbtickersymbol AS symbol
  FROM main.sbticker AS sbticker
), _table_alias_4 AS (
  SELECT DISTINCT
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbdailyprice.sbdpdate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), -2)
      END
    ) AS month,
    _table_alias_1.symbol AS symbol
  FROM main.sbdailyprice AS sbdailyprice
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_1._id = sbdailyprice.sbdptickerid
), _table_alias_5 AS (
  SELECT
    AVG(sbdailyprice.sbdpclose) AS agg_0,
    MAX(sbdailyprice.sbdphigh) AS agg_1,
    MIN(sbdailyprice.sbdplow) AS agg_2,
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbdailyprice.sbdpdate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), (
          2 * -1
        ))
      END
    ) AS month,
    _table_alias_3.symbol AS symbol
  FROM main.sbdailyprice AS sbdailyprice
  LEFT JOIN _table_alias_1 AS _table_alias_3
    ON _table_alias_3._id = sbdailyprice.sbdptickerid
  GROUP BY
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbdailyprice.sbdpdate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), (
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
