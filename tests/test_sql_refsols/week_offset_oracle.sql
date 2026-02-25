SELECT
  sbtxdatetime AS date_time,
  CAST(sbtxdatetime AS DATE) + NUMTODSINTERVAL(7, 'DAY') AS week_adj1,
  CAST(sbtxdatetime AS DATE) - NUMTODSINTERVAL(7, 'DAY') AS week_adj2,
  CAST(sbtxdatetime AS DATE) + NUMTODSINTERVAL(1, 'hour') + NUMTODSINTERVAL(14, 'DAY') AS week_adj3,
  CAST(sbtxdatetime AS DATE) - NUMTODSINTERVAL(1, 'second') + NUMTODSINTERVAL(14, 'DAY') AS week_adj4,
  CAST(sbtxdatetime AS DATE) + NUMTODSINTERVAL(1, 'day') + NUMTODSINTERVAL(14, 'DAY') AS week_adj5,
  CAST(sbtxdatetime AS DATE) - NUMTODSINTERVAL(1, 'minute') + NUMTODSINTERVAL(14, 'DAY') AS week_adj6,
  CAST(sbtxdatetime AS DATE) + NUMTOYMINTERVAL(1, 'month') + NUMTODSINTERVAL(14, 'DAY') AS week_adj7,
  CAST(sbtxdatetime AS DATE) + NUMTOYMINTERVAL(1, 'year') + NUMTODSINTERVAL(14, 'DAY') AS week_adj8
FROM MAIN.SBTRANSACTION
WHERE
  EXTRACT(DAY FROM CAST(sbtxdatetime AS DATE)) > 1
  AND EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATE)) < 2025
