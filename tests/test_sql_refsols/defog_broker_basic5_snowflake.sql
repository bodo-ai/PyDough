SELECT
  sbcustid AS _id
FROM broker.sbcustomer
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM broker.sbtransaction
    WHERE
      sbcustomer.sbcustid = sbtxcustid AND sbtxtype = 'buy'
  )
