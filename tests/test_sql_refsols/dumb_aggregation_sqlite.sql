WITH _s0 AS (
  SELECT
    n_name,
    n_regionkey
  FROM tpch.nation
  ORDER BY
    1
  LIMIT 2
), _s1 AS (
  SELECT
    r_name,
    r_regionkey,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY r_regionkey ORDER BY r_regionkey DESC) - 1.0
        ) - (
          CAST((
            COUNT(r_regionkey) OVER (PARTITION BY r_regionkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN r_regionkey
      ELSE NULL
    END AS avg_expr
  FROM tpch.region
)
SELECT
  _s0.n_name AS nation_name,
  _s1.r_name AS a1,
  _s1.r_name AS a2,
  _s1.r_regionkey AS a3,
  IIF(
    NOT CASE WHEN _s1.r_name <> 'AMERICA' THEN _s1.r_regionkey ELSE NULL END IS NULL,
    1,
    0
  ) AS a4,
  1 AS a5,
  _s1.r_regionkey AS a6,
  _s1.r_name AS a7,
  _s1.avg_expr AS a8
FROM _s0 AS _s0
JOIN _s1 AS _s1
  ON _s0.n_regionkey = _s1.r_regionkey
ORDER BY
  1
