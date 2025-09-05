SELECT
  CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT) AS _expr0,
  SPLIT_PART(sbcustname, ' ', CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p1,
  SPLIT_PART(sbcustname, ' ', 0 - CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p2,
  SPLIT_PART(sbcustemail, '.', CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p3,
  SPLIT_PART(sbcustemail, '.', 0 - CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p4,
  SPLIT_PART(sbcustphone, '-', CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p5,
  SPLIT_PART(sbcustphone, '-', 0 - CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p6,
  SPLIT_PART(sbcustpostalcode, '00', CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p7,
  SPLIT_PART(sbcustpostalcode, '00', 0 - CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p8,
  SPLIT_PART(sbcustname, '!', CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p9,
  SPLIT_PART(sbcustname, '@', 0 - CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p10,
  SPLIT_PART(sbcustname, 'aa', CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p11,
  SPLIT_PART(sbcustname, '#$*', 0 - CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p12,
  SPLIT_PART(sbcustname, '', CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p13,
  SPLIT_PART('', ' ', CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p14,
  SPLIT_PART(sbcustname, ' ', 0) AS p15,
  SPLIT_PART(sbcuststate, sbcuststate, CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p16,
  SPLIT_PART(SPLIT_PART(sbcustphone, '-', 1), '5', CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p17,
  SPLIT_PART(sbcustpostalcode, '0', CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT)) AS p18
FROM main.sbcustomer
WHERE
  CAST(SUBSTRING(sbcustid FROM 2) AS BIGINT) <= 4
ORDER BY
  1 NULLS FIRST
