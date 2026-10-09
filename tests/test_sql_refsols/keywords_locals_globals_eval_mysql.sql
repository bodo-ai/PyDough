SELECT
  `COUNT`.node,
  `CAST`.types,
  2.0 AS expr
FROM keywords.`COUNT` AS `COUNT`
JOIN keywords.`CAST` AS `CAST`
  ON `CAST`.PK_FIELD_NAME = `COUNT`.this
WHERE
  `COUNT`.node = 4071
