WITH _t0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM(_s0.sbtxamount) AS agg_1
  FROM main.sbtransaction AS _s0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM main.sbcustomer AS _s1
      WHERE
        LOWER(_s1.sbcustcountry) = 'usa' AND _s0.sbtxcustid = _s1.sbcustid
    )
    AND _s0.sbtxdatetime < DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
    AND _s0.sbtxdatetime >= DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day',
      '-7 day'
    )
)
SELECT
  CASE WHEN _t0.agg_0 > 0 THEN _t0.agg_0 ELSE NULL END AS n_transactions,
  COALESCE(_t0.agg_1, 0) AS total_amount
FROM _t0 AS _t0
