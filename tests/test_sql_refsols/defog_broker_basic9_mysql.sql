SELECT
  sbcustid AS _id,
  sbcustname AS name
FROM broker.sbCustomer
WHERE
  NOT EXISTS(
    SELECT
      1 AS `1`
    FROM broker.sbTransaction
    WHERE
      sbCustomer.sbcustid = sbtxcustid
  )
