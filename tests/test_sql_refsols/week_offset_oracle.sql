SELECT
  sbtxdatetime AS date_time,
  CAST(sbtxdatetime AS TIMESTAMP) + NUMTODSINTERVAL(1, 'week') AS week_adj1,
  CAST(sbtxdatetime AS TIMESTAMP) + NUMTODSINTERVAL(1, 'week') AS week_adj2,
  CAST(sbtxdatetime AS TIMESTAMP) + NUMTODSINTERVAL(1, 'hour') + NUMTODSINTERVAL(2, 'week') AS week_adj3,
  CAST(sbtxdatetime AS TIMESTAMP) + NUMTODSINTERVAL(1, 'second') + NUMTODSINTERVAL(2, 'week') AS week_adj4,
  CAST(sbtxdatetime AS TIMESTAMP) + NUMTODSINTERVAL(1, 'day') + NUMTODSINTERVAL(2, 'week') AS week_adj5,
  CAST(sbtxdatetime AS TIMESTAMP) + NUMTODSINTERVAL(1, 'minute') + NUMTODSINTERVAL(2, 'week') AS week_adj6,
  CAST(sbtxdatetime AS TIMESTAMP) + NUMTOYMINTERVAL(1, 'month') + NUMTODSINTERVAL(2, 'week') AS week_adj7,
  CAST(sbtxdatetime AS TIMESTAMP) + NUMTOYMINTERVAL(1, 'year') + NUMTODSINTERVAL(2, 'week') AS week_adj8
FROM MAIN.SBTRANSACTION
WHERE
  EXTRACT(DAY FROM CAST(sbtxdatetime AS DATE)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATE)) < 2025
