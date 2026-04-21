SELECT
  REGEXP_SUBSTR(
    sbcustname,
    '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' '))
      ) + 1 >= (
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' '))
      ) + 1
      AND (
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' '))
      ) >= 0
      THEN (
        LENGTH(sbcustname) - LENGTH(REPLACE(sbcustname, ' '))
      ) + 1
      ELSE NULL
    END,
    NULL,
    1
  ) AS last_name
FROM MAIN.SBCUSTOMER
WHERE
  sbcustname = 'Alex Rodriguez'
