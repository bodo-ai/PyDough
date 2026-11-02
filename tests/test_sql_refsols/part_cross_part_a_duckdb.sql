WITH _s0 AS (
  SELECT DISTINCT
    sbtickerexchange
  FROM main.sbticker
), _s1 AS (
  SELECT DISTINCT
    sbcuststate
  FROM main.sbcustomer
), _s11 AS (
  SELECT
    _s3.sbcuststate,
    _s2.sbtickerexchange,
    COUNT(*) AS n_rows
  FROM _s0 AS _s2
  CROSS JOIN _s1 AS _s3
  JOIN main.sbcustomer AS sbcustomer
    ON _s3.sbcuststate = sbcustomer.sbcuststate
  JOIN main.sbtransaction AS sbtransaction
    ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  JOIN main.sbticker AS sbticker
    ON _s2.sbtickerexchange = sbticker.sbtickerexchange
    AND sbticker.sbtickerid = sbtransaction.sbtxtickerid
  GROUP BY
    1,
    2
)
SELECT
  _s1.sbcuststate AS state,
  _s0.sbtickerexchange AS exchange,
  COALESCE(_s11.n_rows, 0) AS n
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
LEFT JOIN _s11 AS _s11
  ON _s0.sbtickerexchange = _s11.sbtickerexchange
  AND _s1.sbcuststate = _s11.sbcuststate
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
