SELECT
  CASE
    WHEN CHAR_LENGTH(sbcustname) = 0
    THEN NULL
    WHEN CHAR_LENGTH(' ') = 0
    THEN CASE WHEN ABS(-1) = 1 THEN sbcustname ELSE NULL END
    WHEN (
      CHAR_LENGTH(sbcustname) - CHAR_LENGTH(REPLACE(sbcustname, ' ', ''))
    ) / CHAR_LENGTH(' ') + 1 >= ABS(-1)
    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(sbcustname, ' ', -1), ' ', 1)
    ELSE NULL
  END AS last_name
FROM main.sbcustomer
WHERE
  sbcustname = 'Alex Rodriguez'
