SELECT
  sbcustomer.sbcustid AS _id
FROM main.sbcustomer AS sbcustomer
JOIN main.sbtransaction AS sbtransaction
  ON sbcustomer.sbcustid = sbtransaction.sbtxcustid AND sbtransaction.sbtxtype = 'buy'
