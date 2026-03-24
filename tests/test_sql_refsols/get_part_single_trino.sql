SELECT
  SPLIT_PART(
    sbcustname,
    ' ',
    CAST(CAST((
      LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
    ) AS DOUBLE) AS BIGINT) - -1 + 1
  ) AS last_name
FROM main.sbcustomer
WHERE
  sbcustname = 'Alex Rodriguez'
