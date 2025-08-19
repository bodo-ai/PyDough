WITH _t1 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY 1) - 1.0
        ) - (
          CAST((
            COUNT(1) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN 1
      ELSE NULL
    END AS expr_72,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY 1) - 1.0
        ) - (
          CAST((
            COUNT(2) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN 2
      ELSE NULL
    END AS expr_73,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY 1) - 1.0
        ) - (
          CAST((
            COUNT(-1) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN -1
      ELSE NULL
    END AS expr_74,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY 1) - 1.0
        ) - (
          CAST((
            COUNT(-3) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN -3
      ELSE NULL
    END AS expr_75,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY 1) - 1.0
        ) - (
          CAST((
            COUNT(0) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN 0
      ELSE NULL
    END AS expr_76,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY 1) - 1.0
        ) - (
          CAST((
            COUNT(0.5) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN 0.5
      ELSE NULL
    END AS expr_77,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY 1) - 1.0
        ) - (
          CAST((
            COUNT(NULL) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN NULL
      ELSE NULL
    END AS expr_78,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) DESC) - 1.0
        ) - (
          CAST((
            COUNT(
              LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)
            ) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)
      ELSE NULL
    END AS expr_79,
    CASE
      WHEN CAST(0.9 * COUNT(1) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY 1)
      THEN 1
      ELSE NULL
    END AS expr_80,
    CASE
      WHEN CAST(0.8 * COUNT(2) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY 1)
      THEN 2
      ELSE NULL
    END AS expr_81,
    CASE
      WHEN CAST(0.7 * COUNT(-1) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY 1)
      THEN -1
      ELSE NULL
    END AS expr_82,
    CASE
      WHEN CAST(0.6 * COUNT(-3) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY 1)
      THEN -3
      ELSE NULL
    END AS expr_83,
    CASE
      WHEN CAST(0.5 * COUNT(0) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY 1)
      THEN 0
      ELSE NULL
    END AS expr_84,
    CASE
      WHEN CAST(0.4 * COUNT(0.5) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY 1)
      THEN 0.5
      ELSE NULL
    END AS expr_85,
    CASE
      WHEN CAST(0.30000000000000004 * COUNT(NULL) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY 1)
      THEN NULL
      ELSE NULL
    END AS expr_86,
    CASE
      WHEN CAST(0.19999999999999996 * COUNT(
        LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)
      ) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) DESC)
      THEN LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)
      ELSE NULL
    END AS expr_87,
    sbtickerexchange
  FROM main.sbticker
)
SELECT
  LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) AS aug_exchange,
  COUNT(*) AS su1,
  COUNT(*) * 2 AS su2,
  COUNT(*) * -1 AS su3,
  COUNT(*) * -3 AS su4,
  0 AS su5,
  COUNT(*) * 0.5 AS su6,
  0 AS su7,
  COALESCE(
    LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END),
    0
  ) AS su8,
  COUNT(*) AS co1,
  COUNT(*) AS co2,
  COUNT(*) AS co3,
  COUNT(*) AS co4,
  COUNT(*) AS co5,
  COUNT(*) AS co6,
  0 AS co7,
  COUNT(*) * CAST(NOT (
    LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) IS NULL
  ) AS INTEGER) AS co8,
  1 AS nd1,
  1 AS nd2,
  1 AS nd3,
  1 AS nd4,
  1 AS nd5,
  1 AS nd6,
  0 AS nd7,
  CAST(NOT (
    LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) IS NULL
  ) AS INTEGER) AS nd8,
  1 AS av1,
  2 AS av2,
  -1 AS av3,
  -3 AS av4,
  0 AS av5,
  0.5 AS av6,
  NULL AS av7,
  LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) AS av8,
  1 AS mi1,
  2 AS mi2,
  -1 AS mi3,
  -3 AS mi4,
  0 AS mi5,
  0.5 AS mi6,
  NULL AS mi7,
  LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) AS mi8,
  1 AS ma1,
  2 AS ma2,
  -1 AS ma3,
  -3 AS ma4,
  0 AS ma5,
  0.5 AS ma6,
  NULL AS ma7,
  LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) AS ma8,
  1 AS an1,
  2 AS an2,
  -1 AS an3,
  -3 AS an4,
  0 AS an5,
  0.5 AS an6,
  NULL AS an7,
  LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) AS an8,
  AVG(expr_72) AS me1,
  AVG(expr_73) AS me2,
  AVG(expr_74) AS me3,
  AVG(expr_75) AS me4,
  AVG(expr_76) AS me5,
  AVG(expr_77) AS me6,
  AVG(expr_78) AS me7,
  AVG(expr_79) AS me8,
  MAX(expr_80) AS qu1,
  MAX(expr_81) AS qu2,
  MAX(expr_82) AS qu3,
  MAX(expr_83) AS qu4,
  MAX(expr_84) AS qu5,
  MAX(expr_85) AS qu6,
  MAX(expr_86) AS qu7,
  MAX(expr_87) AS qu8
FROM _t1
GROUP BY
  LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)
ORDER BY
  aug_exchange
