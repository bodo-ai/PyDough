SELECT
  REGEXP_SUBSTR(
    SBCUSTNAME,
    '(.*?)(' || REGEXP_REPLACE(' ', '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
    1,
    CASE
      WHEN (
        LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, ' '))
      ) + 1 >= (
        LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, ' '))
      ) + 1
      AND (
        LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, ' '))
      ) >= 0
      THEN (
        LENGTH(SBCUSTNAME) - LENGTH(REPLACE(SBCUSTNAME, ' '))
      ) + 1
      ELSE NULL
    END,
    NULL,
    1
  ) AS last_name
FROM MAIN.SBCUSTOMER
WHERE
  SBCUSTNAME = 'Alex Rodriguez'
