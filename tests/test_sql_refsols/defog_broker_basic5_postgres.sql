SELECT
  sbcustid AS _id
FROM main.sbcustomer
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.sbtransaction
    WHERE
      sbcustomer.sbcustid = sbtxcustid AND sbtxtype = 'buy'
  )
