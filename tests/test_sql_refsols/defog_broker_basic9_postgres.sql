SELECT
  sbcustid AS _id,
  sbcustname AS name
FROM main.sbcustomer
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.sbtransaction
    WHERE
      sbcustomer.sbcustid = sbtxcustid
  )
