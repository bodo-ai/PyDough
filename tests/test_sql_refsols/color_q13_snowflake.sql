WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM shpmnts
  WHERE
    colid = 'avocado'
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM shpmnts
  WHERE
    colid = 'blue_bell'
), _s3 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM shpmnts
  WHERE
    colid = 'carmine'
), _s5 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM shpmnts
  WHERE
    colid = 'deep_fuchsia'
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM shpmnts
  WHERE
    colid = 'mode_beige'
)
SELECT
  _s0.n_rows AS n_avocado,
  _s1.n_rows AS n_blue_bell,
  _s3.n_rows AS n_carmine,
  _s5.n_rows AS n_deep_fuchsia,
  _s7.n_rows AS n_mode_beige
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
CROSS JOIN _s3 AS _s3
CROSS JOIN _s5 AS _s5
CROSS JOIN _s7 AS _s7
