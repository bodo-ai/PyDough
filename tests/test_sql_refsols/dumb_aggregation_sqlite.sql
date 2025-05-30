WITH _s0 AS (
  SELECT
    n_name AS nation_name,
    n_name AS name,
    n_regionkey AS region_key
  FROM tpch.nation
  ORDER BY
    name
  LIMIT 2
), _s1 AS (
  SELECT
    r_name AS a1,
    r_name AS a2,
    COALESCE(r_regionkey, 0) AS a3,
    IIF(NOT CASE WHEN r_name <> 'AMERICA' THEN r_regionkey ELSE NULL END IS NULL, 1, 0) AS a4,
    1 AS a5,
    r_regionkey AS a6,
    r_name AS a7,
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
    END AS a8,
    r_regionkey AS key
  FROM tpch.region
)
SELECT
  _s0.nation_name,
  _s1.a1,
  _s1.a2,
  _s1.a3,
  _s1.a4,
  _s1.a5,
  _s1.a6,
  _s1.a7,
  _s1.a8
FROM _s0 AS _s0
JOIN _s1 AS _s1
  ON _s0.region_key = _s1.key
ORDER BY
  _s0.name
