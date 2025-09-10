SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  (
    NOT c_addr IS NULL OR c_birthday IS NULL
  )
  AND (
    NOT c_addr IS NULL OR c_phone LIKE '%5'
  )
  AND (
    NOT c_birthday IS NULL OR c_phone LIKE '%5'
  )
  AND (
    c_birthday IS NULL OR c_fname LIKE '%a' OR c_fname LIKE '%e' OR c_fname LIKE '%s'
  )
  AND (
    c_birthday IS NULL OR c_lname <> 'lopez'
  )
  AND (
    c_fname LIKE '%a' OR c_fname LIKE '%e' OR c_fname LIKE '%s' OR c_phone LIKE '%5'
  )
  AND (
    c_lname <> 'lopez' OR c_phone LIKE '%5'
  )
