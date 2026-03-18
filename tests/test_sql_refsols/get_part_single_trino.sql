SELECT
  SPLIT_PART(
    sbcustname,
    ' ',
    CASE
      WHEN LENGTH(' ') = 0
      THEN 0
      ELSE CAST(CAST((
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' ', ''))
      ) AS DOUBLE) / LENGTH(' ') AS BIGINT)
    END - -1 + 1
  ) AS last_name
FROM main.sbcustomer
WHERE
  sbcustname = 'Alex Rodriguez'
