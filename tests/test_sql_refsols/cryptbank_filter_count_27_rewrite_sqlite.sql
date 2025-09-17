SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  (
    DATE(c_birthday, '+472 days') IS NULL
    OR LOWER(c_fname) LIKE '%a'
    OR LOWER(c_fname) LIKE '%e'
    OR LOWER(c_fname) LIKE '%s'
  )
  AND (
    DATE(c_birthday, '+472 days') IS NULL
    OR NOT (
      SUBSTRING(c_addr, -1) || SUBSTRING(c_addr, 1, LENGTH(c_addr) - 1)
    ) IS NULL
  )
  AND (
    DATE(c_birthday, '+472 days') IS NULL OR c_lname <> UPPER('lopez')
  )
  AND (
    LOWER(c_fname) LIKE '%a'
    OR LOWER(c_fname) LIKE '%e'
    OR LOWER(c_fname) LIKE '%s'
    OR REPLACE(REPLACE(REPLACE(c_phone, '9', '*'), '0', '9'), '*', '0') LIKE '%5'
  )
  AND (
    NOT (
      SUBSTRING(c_addr, -1) || SUBSTRING(c_addr, 1, LENGTH(c_addr) - 1)
    ) IS NULL
    OR REPLACE(REPLACE(REPLACE(c_phone, '9', '*'), '0', '9'), '*', '0') LIKE '%5'
  )
  AND (
    NOT DATE(c_birthday, '+472 days') IS NULL
    OR REPLACE(REPLACE(REPLACE(c_phone, '9', '*'), '0', '9'), '*', '0') LIKE '%5'
  )
  AND (
    REPLACE(REPLACE(REPLACE(c_phone, '9', '*'), '0', '9'), '*', '0') LIKE '%5'
    OR c_lname <> UPPER('lopez')
  )
