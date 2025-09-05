SELECT
  SPLIT_PART(sbcustname, ' ', -1) AS last_name
FROM main.sbcustomer
WHERE
  sbcustname = 'Alex Rodriguez'
