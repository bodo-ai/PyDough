SELECT
  CASE
    WHEN CHAR_LENGTH(sbCustName) = 0
    THEN ''
    WHEN (
      CHAR_LENGTH(sbCustName) - CHAR_LENGTH(REPLACE(sbCustName, ' ', ''))
    ) + 1 >= ABS(-1)
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbCustName, ' ', -1), ' ', 1)
    ELSE ''
  END AS last_name
FROM main.sbCustomer
WHERE
  sbCustName = 'Alex Rodriguez'
