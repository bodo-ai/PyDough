WITH _s0 AS (
  SELECT DISTINCT
    sbtickerexchange
  FROM main.sbticker
), _s9 AS (
  SELECT
    sbcustomer.sbcustid,
    _s2.sbtickerexchange,
    COUNT(*) AS n_rows
  FROM _s0 AS _s2
  CROSS JOIN main.sbcustomer AS sbcustomer
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
  sbcustomer.sbcuststate AS state,
  _s0.sbtickerexchange AS exchange,
  SUM(_s9.n_rows) AS n
FROM _s0 AS _s0
CROSS JOIN main.sbcustomer AS sbcustomer
JOIN _s9 AS _s9
  ON _s0.sbtickerexchange = _s9.sbtickerexchange
  AND _s9.sbcustid = sbcustomer.sbcustid
GROUP BY
  1,
  2
ORDER BY
  1,
  2
