WITH _t1 AS (
  SELECT
    sbtickerexchange,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY '1') - 1.0
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
          ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY '1') - 1.0
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
          ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY '1') - 1.0
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
          ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY '1') - 1.0
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
          ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY '1') - 1.0
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
          ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY '1') - 1.0
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
      WHEN CAST(0.9 * COUNT(1) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY '1')
      THEN 1
      ELSE NULL
    END AS expr_80,
    CASE
      WHEN CAST(0.8 * COUNT(2) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY '1')
      THEN 2
      ELSE NULL
    END AS expr_81,
    CASE
      WHEN CAST(0.7 * COUNT(-1) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY '1')
      THEN -1
      ELSE NULL
    END AS expr_82,
    CASE
      WHEN CAST(0.6 * COUNT(-3) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY '1')
      THEN -3
      ELSE NULL
    END AS expr_83,
    CASE
      WHEN CAST(0.5 * COUNT(0) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY '1')
      THEN 0
      ELSE NULL
    END AS expr_84,
    CASE
      WHEN CAST(0.4 * COUNT(0.5) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY '1')
      THEN 0.5
      ELSE NULL
    END AS expr_85,
    CASE
      WHEN CAST(0.19999999999999996 * COUNT(
        LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)
      ) OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)) AS INTEGER) < ROW_NUMBER() OVER (PARTITION BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) ORDER BY LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) DESC)
      THEN LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END)
      ELSE NULL
    END AS expr_87
  FROM main.sbticker
), _t0 AS (
  SELECT
    LENGTH(CASE WHEN sbtickerexchange <> 'NYSE Arca' THEN sbtickerexchange ELSE NULL END) AS aug_exchange,
    AVG(expr_72) AS avg_expr_72,
    AVG(expr_73) AS avg_expr_73,
    AVG(expr_74) AS avg_expr_74,
    AVG(expr_75) AS avg_expr_75,
    AVG(expr_76) AS avg_expr_76,
    AVG(expr_77) AS avg_expr_77,
    AVG(expr_79) AS avg_expr_79,
    MAX(expr_80) AS max_expr_80,
    MAX(expr_81) AS max_expr_81,
    MAX(expr_82) AS max_expr_82,
    MAX(expr_83) AS max_expr_83,
    MAX(expr_84) AS max_expr_84,
    MAX(expr_85) AS max_expr_85,
    MAX(expr_87) AS max_expr_87,
    COUNT(*) AS n_rows
  FROM _t1
  GROUP BY
    1
)
SELECT
  aug_exchange,
  n_rows AS su1,
  n_rows * 2 AS su2,
  n_rows * -1 AS su3,
  n_rows * -3 AS su4,
  0 AS su5,
  n_rows * 0.5 AS su6,
  0 AS su7,
  COALESCE(aug_exchange, 0) AS su8,
  n_rows AS co1,
  n_rows AS co2,
  n_rows AS co3,
  n_rows AS co4,
  n_rows AS co5,
  n_rows AS co6,
  0 AS co7,
  n_rows * IIF(NOT aug_exchange IS NULL, 1, 0) AS co8,
  1 AS nd1,
  1 AS nd2,
  1 AS nd3,
  1 AS nd4,
  1 AS nd5,
  1 AS nd6,
  0 AS nd7,
  CAST(NOT aug_exchange IS NULL AS INTEGER) AS nd8,
  1 AS av1,
  2 AS av2,
  -1 AS av3,
  -3 AS av4,
  0 AS av5,
  0.5 AS av6,
  NULL AS av7,
  aug_exchange AS av8,
  1 AS mi1,
  2 AS mi2,
  -1 AS mi3,
  -3 AS mi4,
  0 AS mi5,
  0.5 AS mi6,
  NULL AS mi7,
  aug_exchange AS mi8,
  1 AS ma1,
  2 AS ma2,
  -1 AS ma3,
  -3 AS ma4,
  0 AS ma5,
  0.5 AS ma6,
  NULL AS ma7,
  aug_exchange AS ma8,
  1 AS an1,
  2 AS an2,
  -1 AS an3,
  -3 AS an4,
  0 AS an5,
  0.5 AS an6,
  NULL AS an7,
  aug_exchange AS an8,
  avg_expr_72 AS me1,
  avg_expr_73 AS me2,
  avg_expr_74 AS me3,
  avg_expr_75 AS me4,
  avg_expr_76 AS me5,
  avg_expr_77 AS me6,
  NULL AS me7,
  avg_expr_79 AS me8,
  max_expr_80 AS qu1,
  max_expr_81 AS qu2,
  max_expr_82 AS qu3,
  max_expr_83 AS qu4,
  max_expr_84 AS qu5,
  max_expr_85 AS qu6,
  NULL AS qu7,
  max_expr_87 AS qu8
FROM _t0
ORDER BY
  1
