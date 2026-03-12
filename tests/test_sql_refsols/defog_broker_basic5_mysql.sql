SELECT
  sbcustid AS _id
FROM broker.sbCustomer
WHERE
  EXISTS(
    SELECT
      1 AS `1`
    FROM broker.sbTransaction
    WHERE
      sbCustomer.sbcustid = sbtxcustid AND sbtxtype = 'buy'
  )
