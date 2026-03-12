SELECT
  sbcustid AS _id,
  sbcustname AS name
FROM broker.sbcustomer
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM broker.sbtransaction
    WHERE
      sbcustomer.sbcustid = sbtxcustid
  )
