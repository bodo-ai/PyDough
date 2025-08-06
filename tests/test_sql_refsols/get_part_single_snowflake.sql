SELECT
  SPLIT_PART(sbcustname, ' ', -1) AS last_name
FROM MAIN.SBCUSTOMER
WHERE
  sbcustname = 'Alex Rodriguez'
