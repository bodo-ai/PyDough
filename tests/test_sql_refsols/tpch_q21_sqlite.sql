WITH _t4 AS (
  SELECT
    MAX(_s4.l_linenumber) AS agg_13,
    MAX(_s4.l_orderkey) AS agg_14,
    MAX(_s4.l_suppkey) AS agg_24,
    MAX(_s5.o_orderkey) AS agg_3,
    MAX(_s5.o_orderstatus) AS agg_6
  FROM tpch.lineitem AS _s4
  JOIN tpch.orders AS _s5
    ON _s4.l_orderkey = _s5.o_orderkey
  JOIN tpch.lineitem AS _s8
    ON _s5.o_orderkey = _s8.l_orderkey
  WHERE
    _s4.l_commitdate < _s4.l_receiptdate AND _s4.l_suppkey <> _s8.l_suppkey
  GROUP BY
    _s5.o_orderkey,
    _s4.l_linenumber,
    _s4.l_orderkey
), _s21 AS (
  SELECT
    COUNT(*) AS agg_0,
    _t4.agg_24 AS agg_24
  FROM _t4 AS _t4
  WHERE
    NOT EXISTS(
      SELECT
        1 AS "1"
      FROM tpch.lineitem AS _s11
      JOIN tpch.orders AS _s12
        ON _s11.l_orderkey = _s12.o_orderkey
      JOIN tpch.lineitem AS _s15
        ON _s12.o_orderkey = _s15.l_orderkey AND _s15.l_commitdate < _s15.l_receiptdate
      WHERE
        _s11.l_commitdate < _s11.l_receiptdate
        AND _s11.l_linenumber = _t4.agg_13
        AND _s11.l_orderkey = _t4.agg_14
        AND _s11.l_suppkey <> _s15.l_suppkey
        AND _s12.o_orderkey = _t4.agg_3
    )
    AND _t4.agg_6 = 'F'
  GROUP BY
    _t4.agg_24
)
SELECT
  _s0.s_name AS S_NAME,
  COALESCE(_s21.agg_0, 0) AS NUMWAIT
FROM tpch.supplier AS _s0
JOIN tpch.nation AS _s1
  ON _s0.s_nationkey = _s1.n_nationkey AND _s1.n_name = 'SAUDI ARABIA'
LEFT JOIN _s21 AS _s21
  ON _s0.s_suppkey = _s21.agg_24
ORDER BY
  numwait DESC,
  s_name
LIMIT 10
