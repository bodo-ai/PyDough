WITH "_S0" AS (
  SELECT
    LTRIM(
      NVL2(
        EXTRACT(YEAR FROM CAST(sbdpdate AS DATE)),
        '-' || EXTRACT(YEAR FROM CAST(sbdpdate AS DATE)),
        NULL
      ) || NVL2(
        CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbdpdate AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(sbdpdate AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(sbdpdate AS DATE))), (
            2 * -1
          ))
        END,
        '-' || CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbdpdate AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(sbdpdate AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(sbdpdate AS DATE))), (
            2 * -1
          ))
        END,
        NULL
      ),
      '-'
    ) AS MONTH,
    sbdptickerid AS SBDPTICKERID,
    COUNT(sbdpclose) AS COUNT_SBDPCLOSE,
    MAX(sbdphigh) AS MAX_SBDPHIGH,
    MIN(sbdplow) AS MIN_SBDPLOW,
    SUM(sbdpclose) AS SUM_SBDPCLOSE
  FROM MAIN.SBDAILYPRICE
  GROUP BY
    LTRIM(
      NVL2(
        EXTRACT(YEAR FROM CAST(sbdpdate AS DATE)),
        '-' || EXTRACT(YEAR FROM CAST(sbdpdate AS DATE)),
        NULL
      ) || NVL2(
        CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbdpdate AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(sbdpdate AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(sbdpdate AS DATE))), (
            2 * -1
          ))
        END,
        '-' || CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbdpdate AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(sbdpdate AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(sbdpdate AS DATE))), (
            2 * -1
          ))
        END,
        NULL
      ),
      '-'
    ),
    sbdptickerid
), "_T0" AS (
  SELECT
    "_S0".MONTH,
    SBTICKER.sbtickersymbol AS SBTICKERSYMBOL,
    MAX("_S0".MAX_SBDPHIGH) AS MAX_MAX_SBDPHIGH,
    MIN("_S0".MIN_SBDPLOW) AS MIN_MIN_SBDPLOW,
    SUM("_S0".COUNT_SBDPCLOSE) AS SUM_COUNT_SBDPCLOSE,
    SUM("_S0".SUM_SBDPCLOSE) AS SUM_SUM_SBDPCLOSE
  FROM "_S0" "_S0"
  JOIN MAIN.SBTICKER SBTICKER
    ON SBTICKER.sbtickerid = "_S0".SBDPTICKERID
  GROUP BY
    "_S0".MONTH,
    SBTICKER.sbtickersymbol
)
SELECT
  SBTICKERSYMBOL AS symbol,
  MONTH AS month,
  SUM_SUM_SBDPCLOSE / SUM_COUNT_SBDPCLOSE AS avg_close,
  MAX_MAX_SBDPHIGH AS max_high,
  MIN_MIN_SBDPLOW AS min_low,
  (
    (
      SUM_SUM_SBDPCLOSE / SUM_COUNT_SBDPCLOSE
    ) - LAG(SUM_SUM_SBDPCLOSE / SUM_COUNT_SBDPCLOSE, 1) OVER (PARTITION BY SBTICKERSYMBOL ORDER BY MONTH)
  ) / NULLIF(
    LAG(SUM_SUM_SBDPCLOSE / SUM_COUNT_SBDPCLOSE, 1) OVER (PARTITION BY SBTICKERSYMBOL ORDER BY MONTH),
    0
  ) AS momc
FROM "_T0"
