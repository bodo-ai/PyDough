SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name
FROM main.sbcustomer AS sbcustomer
JOIN main.sbtransaction AS sbtransaction
  ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
