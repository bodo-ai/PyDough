SELECT
  42 - c_key AS key,
  LOWER(c_fname) AS first_name,
  LOWER(c_lname) AS last_name,
  REPLACE(REPLACE(REPLACE(c_phone, '9', '*'), '0', '9'), '*', '0') AS phone_number,
  SUBSTRING(c_email, -1) || SUBSTRING(c_email, 1, LENGTH(c_email) - 1) AS email,
  SUBSTRING(c_addr, -1) || SUBSTRING(c_addr, 1, LENGTH(c_addr) - 1) AS address,
  DATE(c_birthday, '+472 days') AS birthday
FROM crbnk.customers
ORDER BY
  1
LIMIT 3
