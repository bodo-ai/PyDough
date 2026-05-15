SELECT
  COUNT_IF(colid = 'avocado') AS n_avocado,
  COUNT_IF(colid = 'blue_bell') AS n_blue_bell,
  COUNT_IF(colid = 'carmine') AS n_carmine,
  COUNT_IF(colid = 'deep_fuchsia') AS n_deep_fuchsia,
  COUNT_IF(colid = 'mode_beige') AS n_mode_beige
FROM shpmnts
WHERE
  colid = 'avocado'
  OR colid = 'blue_bell'
  OR colid = 'carmine'
  OR colid = 'deep_fuchsia'
  OR colid = 'mode_beige'
