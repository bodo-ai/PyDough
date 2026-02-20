WITH "_S2" AS (
  SELECT
    LTRIM(
      NVL2(
        EXTRACT(YEAR FROM CAST(sbcustjoindate AS DATE)),
        '-' || EXTRACT(YEAR FROM CAST(sbcustjoindate AS DATE)),
        NULL
      ) || NVL2(
        CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATE))), (
            2 * -1
          ))
        END,
        '-' || CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATE))), (
            2 * -1
          ))
        END,
        NULL
      ),
      '-'
    ) AS MONTH,
    COUNT(*) AS N_ROWS
  FROM MAIN.SBCUSTOMER
  WHERE
    sbcustjoindate < TRUNC(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'MONTH')
    AND sbcustjoindate >= TRUNC(SYS_EXTRACT_UTC(SYSTIMESTAMP) + NUMTOYMINTERVAL(6, 'month'), 'MONTH')
  GROUP BY
    LTRIM(
      NVL2(
        EXTRACT(YEAR FROM CAST(sbcustjoindate AS DATE)),
        '-' || EXTRACT(YEAR FROM CAST(sbcustjoindate AS DATE)),
        NULL
      ) || NVL2(
        CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATE))), (
            2 * -1
          ))
        END,
        '-' || CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATE)), 1, 2)
          ELSE SUBSTR(CONCAT('00', EXTRACT(MONTH FROM CAST(sbcustjoindate AS DATE))), (
            2 * -1
          ))
        END,
        NULL
      ),
      '-'
    )
), "_S3" AS (
  SELECT
    LTRIM(
      NVL2(
        EXTRACT(YEAR FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE)),
        '-' || EXTRACT(YEAR FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE)),
        NULL
      ) || NVL2(
        CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE)), 1, 2)
          ELSE SUBSTR(
            CONCAT('00', EXTRACT(MONTH FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE))),
            (
              2 * -1
            )
          )
        END,
        '-' || CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE)), 1, 2)
          ELSE SUBSTR(
            CONCAT('00', EXTRACT(MONTH FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE))),
            (
              2 * -1
            )
          )
        END,
        NULL
      ),
      '-'
    ) AS MONTH,
    AVG(SBTRANSACTION.sbtxamount) AS AVG_SBTXAMOUNT
  FROM MAIN.SBCUSTOMER SBCUSTOMER
  JOIN MAIN.SBTRANSACTION SBTRANSACTION
    ON EXTRACT(MONTH FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE)) = EXTRACT(MONTH FROM CAST(SBTRANSACTION.sbtxdatetime AS DATE))
    AND EXTRACT(YEAR FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE)) = EXTRACT(YEAR FROM CAST(SBTRANSACTION.sbtxdatetime AS DATE))
    AND SBCUSTOMER.sbcustid = SBTRANSACTION.sbtxcustid
  WHERE
    SBCUSTOMER.sbcustjoindate < TRUNC(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'MONTH')
    AND SBCUSTOMER.sbcustjoindate >= TRUNC(SYS_EXTRACT_UTC(SYSTIMESTAMP) + NUMTOYMINTERVAL(6, 'month'), 'MONTH')
  GROUP BY
    LTRIM(
      NVL2(
        EXTRACT(YEAR FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE)),
        '-' || EXTRACT(YEAR FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE)),
        NULL
      ) || NVL2(
        CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE)), 1, 2)
          ELSE SUBSTR(
            CONCAT('00', EXTRACT(MONTH FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE))),
            (
              2 * -1
            )
          )
        END,
        '-' || CASE
          WHEN LENGTH(EXTRACT(MONTH FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE))) >= 2
          THEN SUBSTR(EXTRACT(MONTH FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE)), 1, 2)
          ELSE SUBSTR(
            CONCAT('00', EXTRACT(MONTH FROM CAST(SBCUSTOMER.sbcustjoindate AS DATE))),
            (
              2 * -1
            )
          )
        END,
        NULL
      ),
      '-'
    )
)
SELECT
  "_S2".MONTH AS month,
  "_S2".N_ROWS AS customer_signups,
  "_S3".AVG_SBTXAMOUNT AS avg_tx_amount
FROM "_S2" "_S2"
LEFT JOIN "_S3" "_S3"
  ON "_S2".MONTH = "_S3".MONTH
