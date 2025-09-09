SELECT
  c_key AS key,
  c_fname AS first_name,
  c_lname AS last_name,
  c_phone AS phone_number,
  c_email AS email,
  c_addr AS address,
  c_birthday AS birthday
FROM crbnk.customers
ORDER BY
  1
LIMIT 3
