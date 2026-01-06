SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  (
    DATE(c_birthday, '+472 days') IS NULL
    OR NOT (
      SUBSTRING(c_addr, -1) || SUBSTRING(c_addr, 1, LENGTH(c_addr) - 1)
    ) IS NULL
  )
  AND (
    DATE(c_birthday, '+472 days') IS NULL
    OR c_fname IN ('ALICE', 'GRACE', 'LUKE', 'MARIA', 'OLIVIA', 'QUEENIE', 'SOPHIA')
    OR c_fname IN ('JAMES', 'NICHOLAS', 'THOMAS')
  )
  AND (
    DATE(c_birthday, '+472 days') IS NULL OR c_lname <> 'LOPEZ'
  )
  AND (
    NOT (
      SUBSTRING(c_addr, -1) || SUBSTRING(c_addr, 1, LENGTH(c_addr) - 1)
    ) IS NULL
    OR c_phone IN ('555-091-2345', '555-901-2345')
  )
  AND (
    NOT DATE(c_birthday, '+472 days') IS NULL
    OR c_phone IN ('555-091-2345', '555-901-2345')
  )
  AND (
    c_fname IN ('ALICE', 'GRACE', 'LUKE', 'MARIA', 'OLIVIA', 'QUEENIE', 'SOPHIA')
    OR c_fname IN ('JAMES', 'NICHOLAS', 'THOMAS')
    OR c_phone IN ('555-091-2345', '555-901-2345')
  )
  AND (
    c_lname <> 'LOPEZ' OR c_phone IN ('555-091-2345', '555-901-2345')
  )
