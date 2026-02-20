WITH "_T1" AS (
  SELECT
    sbtickerexchange AS SBTICKEREXCHANGE,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) ORDER BY '1') - 1.0
        ) - (
          (
            COUNT(1) OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca'))) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN 1
      ELSE NULL
    END AS EXPR_72,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) ORDER BY '1') - 1.0
        ) - (
          (
            COUNT(2) OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca'))) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN 2
      ELSE NULL
    END AS EXPR_73,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) ORDER BY '1') - 1.0
        ) - (
          (
            COUNT(-1) OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca'))) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN -1
      ELSE NULL
    END AS EXPR_74,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) ORDER BY '1') - 1.0
        ) - (
          (
            COUNT(-3) OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca'))) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN -3
      ELSE NULL
    END AS EXPR_75,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) ORDER BY '1') - 1.0
        ) - (
          (
            COUNT(0) OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca'))) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN 0
      ELSE NULL
    END AS EXPR_76,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) ORDER BY '1') - 1.0
        ) - (
          (
            COUNT(0.5) OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca'))) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN 0.5
      ELSE NULL
    END AS EXPR_77,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) ORDER BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) DESC NULLS LAST) - 1.0
        ) - (
          (
            COUNT(LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca'))) OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca'))) - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca'))
      ELSE NULL
    END AS EXPR_79,
    CASE
      WHEN FLOOR(0.9 * COUNT(1) OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')))) < ROW_NUMBER() OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) ORDER BY '1')
      THEN 1
      ELSE NULL
    END AS EXPR_80,
    CASE
      WHEN FLOOR(0.8 * COUNT(2) OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')))) < ROW_NUMBER() OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) ORDER BY '1')
      THEN 2
      ELSE NULL
    END AS EXPR_81,
    CASE
      WHEN FLOOR(
        0.7 * COUNT(-1) OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')))
      ) < ROW_NUMBER() OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) ORDER BY '1')
      THEN -1
      ELSE NULL
    END AS EXPR_82,
    CASE
      WHEN FLOOR(
        0.6 * COUNT(-3) OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')))
      ) < ROW_NUMBER() OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) ORDER BY '1')
      THEN -3
      ELSE NULL
    END AS EXPR_83,
    CASE
      WHEN FLOOR(0.5 * COUNT(0) OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')))) < ROW_NUMBER() OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) ORDER BY '1')
      THEN 0
      ELSE NULL
    END AS EXPR_84,
    CASE
      WHEN FLOOR(
        0.4 * COUNT(0.5) OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')))
      ) < ROW_NUMBER() OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) ORDER BY '1')
      THEN 0.5
      ELSE NULL
    END AS EXPR_85,
    CASE
      WHEN FLOOR(
        0.2 * COUNT(LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca'))) OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')))
      ) < ROW_NUMBER() OVER (PARTITION BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) ORDER BY LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca')) DESC NULLS LAST)
      THEN LENGTH(NULLIF(sbtickerexchange, 'NYSE Arca'))
      ELSE NULL
    END AS EXPR_87
  FROM MAIN.SBTICKER
), "_T0" AS (
  SELECT
    LENGTH(CASE WHEN SBTICKEREXCHANGE <> 'NYSE Arca' THEN SBTICKEREXCHANGE ELSE NULL END) AS AUG_EXCHANGE,
    AVG(EXPR_72) AS AVG_EXPR_72,
    AVG(EXPR_73) AS AVG_EXPR_73,
    AVG(EXPR_74) AS AVG_EXPR_74,
    AVG(EXPR_75) AS AVG_EXPR_75,
    AVG(EXPR_76) AS AVG_EXPR_76,
    AVG(EXPR_77) AS AVG_EXPR_77,
    AVG(EXPR_79) AS AVG_EXPR_79,
    MAX(EXPR_80) AS MAX_EXPR_80,
    MAX(EXPR_81) AS MAX_EXPR_81,
    MAX(EXPR_82) AS MAX_EXPR_82,
    MAX(EXPR_83) AS MAX_EXPR_83,
    MAX(EXPR_84) AS MAX_EXPR_84,
    MAX(EXPR_85) AS MAX_EXPR_85,
    MAX(EXPR_87) AS MAX_EXPR_87,
    COUNT(*) AS N_ROWS
  FROM "_T1"
  GROUP BY
    LENGTH(CASE WHEN SBTICKEREXCHANGE <> 'NYSE Arca' THEN SBTICKEREXCHANGE ELSE NULL END)
)
SELECT
  AUG_EXCHANGE AS aug_exchange,
  N_ROWS AS su1,
  N_ROWS * 2 AS su2,
  N_ROWS * -1 AS su3,
  N_ROWS * -3 AS su4,
  0 AS su5,
  N_ROWS * 0.5 AS su6,
  0 AS su7,
  COALESCE(AUG_EXCHANGE, 0) AS su8,
  N_ROWS AS co1,
  N_ROWS AS co2,
  N_ROWS AS co3,
  N_ROWS AS co4,
  N_ROWS AS co5,
  N_ROWS AS co6,
  0 AS co7,
  N_ROWS * CASE WHEN NOT AUG_EXCHANGE IS NULL THEN 1 ELSE 0 END AS co8,
  1 AS nd1,
  1 AS nd2,
  1 AS nd3,
  1 AS nd4,
  1 AS nd5,
  1 AS nd6,
  0 AS nd7,
  CAST(NOT AUG_EXCHANGE IS NULL AS INT) AS nd8,
  1 AS av1,
  2 AS av2,
  -1 AS av3,
  -3 AS av4,
  0 AS av5,
  0.5 AS av6,
  NULL AS av7,
  AUG_EXCHANGE AS av8,
  1 AS mi1,
  2 AS mi2,
  -1 AS mi3,
  -3 AS mi4,
  0 AS mi5,
  0.5 AS mi6,
  NULL AS mi7,
  AUG_EXCHANGE AS mi8,
  1 AS ma1,
  2 AS ma2,
  -1 AS ma3,
  -3 AS ma4,
  0 AS ma5,
  0.5 AS ma6,
  NULL AS ma7,
  AUG_EXCHANGE AS ma8,
  1 AS an1,
  2 AS an2,
  -1 AS an3,
  -3 AS an4,
  0 AS an5,
  0.5 AS an6,
  NULL AS an7,
  AUG_EXCHANGE AS an8,
  AVG_EXPR_72 AS me1,
  AVG_EXPR_73 AS me2,
  AVG_EXPR_74 AS me3,
  AVG_EXPR_75 AS me4,
  AVG_EXPR_76 AS me5,
  AVG_EXPR_77 AS me6,
  NULL AS me7,
  AVG_EXPR_79 AS me8,
  MAX_EXPR_80 AS qu1,
  MAX_EXPR_81 AS qu2,
  MAX_EXPR_82 AS qu3,
  MAX_EXPR_83 AS qu4,
  MAX_EXPR_84 AS qu5,
  MAX_EXPR_85 AS qu6,
  NULL AS qu7,
  MAX_EXPR_87 AS qu8
FROM "_T0"
ORDER BY
  1 NULLS FIRST
