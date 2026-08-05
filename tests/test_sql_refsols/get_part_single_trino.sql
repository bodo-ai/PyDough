SELECT
  CASE
    WHEN (
      CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
      ) AS DOUBLE) AS BIGINT) + 1
    ) < ABS(-1)
    THEN ''
    WHEN (
      CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
      ) AS DOUBLE) AS BIGINT) + 1
    ) >= ABS(-1)
    THEN SPLIT_PART(
      sbcustname,
      ' ',
      CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
      ) AS DOUBLE) AS BIGINT) + 1
    )
    ELSE SPLIT_PART(sbcustname, ' ', -1)
  END AS last_name
FROM main.sbcustomer
WHERE
  sbcustname = 'Alex Rodriguez'
