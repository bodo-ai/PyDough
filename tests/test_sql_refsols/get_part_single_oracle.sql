SELECT
  REGEXP_SUBSTR(sbcustname, '[^ ]+', 1, REGEXP_COUNT(sbcustname, ' ') + 1) AS last_name
FROM MAIN.SBCUSTOMER
WHERE
  sbcustname = 'Alex Rodriguez'
