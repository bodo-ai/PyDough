SELECT
  asia_nations_t23.nkey,
  asia_nations_t23.nname,
  mults_t23.mult
FROM asia_nations_t23 AS asia_nations_t23
CROSS JOIN mults_t23 AS mults_t23
ORDER BY
  1,
  3
