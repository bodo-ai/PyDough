SELECT
  sbcustname AS name,
  SUBSTRING(
    sbcustname,
    CASE
      WHEN (
        LENGTH(sbcustname) + -1
      ) < 1
      THEN 1
      ELSE (
        LENGTH(sbcustname) + -1
      )
    END
  ) AS neg_none_step,
  SUBSTRING(sbcustname, 4) AS pos_none_step,
  SUBSTRING(sbcustname, 1, 3) AS none_pos_step,
  SUBSTRING(
    sbcustname,
    1,
    CASE
      WHEN (
        LENGTH(sbcustname) + -2
      ) < 0
      THEN 0
      ELSE (
        LENGTH(sbcustname) + -2
      )
    END
  ) AS none_neg_step,
  SUBSTRING(sbcustname, 3, 2) AS pos_pos_step,
  SUBSTRING(
    sbcustname,
    3,
    CASE
      WHEN (
        LENGTH(sbcustname) + -1
      ) < 1
      THEN 0
      ELSE CASE
        WHEN (
          (
            LENGTH(sbcustname) + -1
          ) - 3
        ) <= 0
        THEN 0
        ELSE (
          LENGTH(sbcustname) + -1
        ) - 3
      END
    END
  ) AS pos_neg_step,
  SUBSTRING(
    sbcustname,
    CASE
      WHEN (
        LENGTH(sbcustname) + -11
      ) < 1
      THEN 1
      ELSE (
        LENGTH(sbcustname) + -11
      )
    END,
    CASE
      WHEN (
        3 - CASE
          WHEN (
            LENGTH(sbcustname) + -11
          ) < 1
          THEN 1
          ELSE (
            LENGTH(sbcustname) + -11
          )
        END
      ) <= 0
      THEN 0
      ELSE 3 - CASE
        WHEN (
          LENGTH(sbcustname) + -11
        ) < 1
        THEN 1
        ELSE (
          LENGTH(sbcustname) + -11
        )
      END
    END
  ) AS neg_pos_step,
  SUBSTRING(
    sbcustname,
    CASE
      WHEN (
        LENGTH(sbcustname) + -3
      ) < 1
      THEN 1
      ELSE (
        LENGTH(sbcustname) + -3
      )
    END,
    CASE
      WHEN (
        LENGTH(sbcustname) + -1
      ) < 1
      THEN 0
      ELSE (
        LENGTH(sbcustname) + -1
      ) - CASE
        WHEN (
          LENGTH(sbcustname) + -3
        ) < 1
        THEN 1
        ELSE (
          LENGTH(sbcustname) + -3
        )
      END
    END
  ) AS neg_neg_step,
  SUBSTRING(
    sbcustname,
    2,
    CASE
      WHEN (
        LENGTH(sbcustname) + 0
      ) < 1
      THEN 0
      ELSE CASE
        WHEN (
          (
            LENGTH(sbcustname) + 0
          ) - 2
        ) <= 0
        THEN 0
        ELSE (
          LENGTH(sbcustname) + 0
        ) - 2
      END
    END
  ) AS inbetween_chars,
  SUBSTRING(sbcustname, 3, 0) AS empty1,
  '' AS empty2,
  '' AS empty3,
  '' AS empty4,
  SUBSTRING(sbcustname, 101, 100) AS oob1,
  SUBSTRING(
    sbcustname,
    CASE
      WHEN (
        LENGTH(sbcustname) + -199
      ) < 1
      THEN 1
      ELSE (
        LENGTH(sbcustname) + -199
      )
    END,
    CASE
      WHEN (
        LENGTH(sbcustname) + -99
      ) < 1
      THEN 0
      ELSE (
        LENGTH(sbcustname) + -99
      ) - CASE
        WHEN (
          LENGTH(sbcustname) + -199
        ) < 1
        THEN 1
        ELSE (
          LENGTH(sbcustname) + -199
        )
      END
    END
  ) AS oob2,
  SUBSTRING(sbcustname, 101) AS oob3,
  SUBSTRING(
    sbcustname,
    CASE
      WHEN (
        LENGTH(sbcustname) + -199
      ) < 1
      THEN 1
      ELSE (
        LENGTH(sbcustname) + -199
      )
    END
  ) AS oob4,
  SUBSTRING(sbcustname, 1, 100) AS oob5,
  SUBSTRING(
    sbcustname,
    1,
    CASE
      WHEN (
        LENGTH(sbcustname) + -200
      ) < 0
      THEN 0
      ELSE (
        LENGTH(sbcustname) + -200
      )
    END
  ) AS oob6,
  SUBSTRING(
    sbcustname,
    101,
    CASE
      WHEN (
        LENGTH(sbcustname) + -199
      ) < 1
      THEN 0
      ELSE CASE
        WHEN (
          (
            LENGTH(sbcustname) + -199
          ) - 101
        ) <= 0
        THEN 0
        ELSE (
          LENGTH(sbcustname) + -199
        ) - 101
      END
    END
  ) AS oob7,
  SUBSTRING(
    sbcustname,
    CASE
      WHEN (
        LENGTH(sbcustname) + -199
      ) < 1
      THEN 1
      ELSE (
        LENGTH(sbcustname) + -199
      )
    END,
    CASE
      WHEN (
        101 - CASE
          WHEN (
            LENGTH(sbcustname) + -199
          ) < 1
          THEN 1
          ELSE (
            LENGTH(sbcustname) + -199
          )
        END
      ) <= 0
      THEN 0
      ELSE 101 - CASE
        WHEN (
          LENGTH(sbcustname) + -199
        ) < 1
        THEN 1
        ELSE (
          LENGTH(sbcustname) + -199
        )
      END
    END
  ) AS oob8,
  SUBSTRING(
    sbcustname,
    101,
    CASE
      WHEN (
        LENGTH(sbcustname) + 0
      ) < 1
      THEN 0
      ELSE CASE
        WHEN (
          (
            LENGTH(sbcustname) + 0
          ) - 101
        ) <= 0
        THEN 0
        ELSE (
          LENGTH(sbcustname) + 0
        ) - 101
      END
    END
  ) AS oob9,
  SUBSTRING(
    sbcustname,
    CASE
      WHEN (
        LENGTH(sbcustname) + -99
      ) < 1
      THEN 1
      ELSE (
        LENGTH(sbcustname) + -99
      )
    END,
    CASE
      WHEN (
        LENGTH(sbcustname) + 0
      ) < 1
      THEN 0
      ELSE (
        LENGTH(sbcustname) + 0
      ) - CASE
        WHEN (
          LENGTH(sbcustname) + -99
        ) < 1
        THEN 1
        ELSE (
          LENGTH(sbcustname) + -99
        )
      END
    END
  ) AS oob10,
  SUBSTRING(
    sbcustname,
    CASE
      WHEN (
        LENGTH(sbcustname) + -2
      ) < 1
      THEN 1
      ELSE (
        LENGTH(sbcustname) + -2
      )
    END,
    CASE
      WHEN (
        101 - CASE
          WHEN (
            LENGTH(sbcustname) + -2
          ) < 1
          THEN 1
          ELSE (
            LENGTH(sbcustname) + -2
          )
        END
      ) <= 0
      THEN 0
      ELSE 101 - CASE
        WHEN (
          LENGTH(sbcustname) + -2
        ) < 1
        THEN 1
        ELSE (
          LENGTH(sbcustname) + -2
        )
      END
    END
  ) AS oob11,
  '' AS oob12,
  SUBSTRING(sbcustname, 1, 0) AS zero1,
  SUBSTRING(sbcustname, 1, 1) AS zero2,
  SUBSTRING(
    sbcustname,
    CASE
      WHEN (
        LENGTH(sbcustname) + 0
      ) < 1
      THEN 1
      ELSE (
        LENGTH(sbcustname) + 0
      )
    END,
    CASE
      WHEN (
        1 - CASE
          WHEN (
            LENGTH(sbcustname) + 0
          ) < 1
          THEN 1
          ELSE (
            LENGTH(sbcustname) + 0
          )
        END
      ) <= 0
      THEN 0
      ELSE 1 - CASE
        WHEN (
          LENGTH(sbcustname) + 0
        ) < 1
        THEN 1
        ELSE (
          LENGTH(sbcustname) + 0
        )
      END
    END
  ) AS zero3,
  '' AS zero4,
  SUBSTRING(
    sbcustname,
    1,
    CASE
      WHEN (
        LENGTH(sbcustname) + 0
      ) < 1
      THEN 0
      ELSE CASE
        WHEN (
          (
            LENGTH(sbcustname) + 0
          ) - 1
        ) <= 0
        THEN 0
        ELSE (
          LENGTH(sbcustname) + 0
        ) - 1
      END
    END
  ) AS zero5,
  SUBSTRING(
    sbcustname,
    1,
    CASE
      WHEN (
        LENGTH(sbcustname) + -19
      ) < 1
      THEN 0
      ELSE CASE
        WHEN (
          (
            LENGTH(sbcustname) + -19
          ) - 1
        ) <= 0
        THEN 0
        ELSE (
          LENGTH(sbcustname) + -19
        ) - 1
      END
    END
  ) AS zero6,
  SUBSTRING(sbcustname, 1, 100) AS zero7,
  '' AS zero8,
  SUBSTRING(
    sbcustname,
    CASE
      WHEN (
        LENGTH(sbcustname) + -19
      ) < 1
      THEN 1
      ELSE (
        LENGTH(sbcustname) + -19
      )
    END,
    CASE
      WHEN (
        1 - CASE
          WHEN (
            LENGTH(sbcustname) + -19
          ) < 1
          THEN 1
          ELSE (
            LENGTH(sbcustname) + -19
          )
        END
      ) <= 0
      THEN 0
      ELSE 1 - CASE
        WHEN (
          LENGTH(sbcustname) + -19
        ) < 1
        THEN 1
        ELSE (
          LENGTH(sbcustname) + -19
        )
      END
    END
  ) AS zero9,
  SUBSTRING(
    sbcustname,
    CASE
      WHEN (
        LENGTH(sbcustname) + -1
      ) < 1
      THEN 1
      ELSE (
        LENGTH(sbcustname) + -1
      )
    END
  ) AS wo_step1,
  SUBSTRING(sbcustname, 4) AS wo_step2,
  SUBSTRING(sbcustname, 1, 3) AS wo_step3,
  SUBSTRING(
    sbcustname,
    1,
    CASE
      WHEN (
        LENGTH(sbcustname) + -2
      ) < 0
      THEN 0
      ELSE (
        LENGTH(sbcustname) + -2
      )
    END
  ) AS wo_step4,
  SUBSTRING(sbcustname, 3, 2) AS wo_step5,
  SUBSTRING(
    sbcustname,
    3,
    CASE
      WHEN (
        LENGTH(sbcustname) + -1
      ) < 1
      THEN 0
      ELSE CASE
        WHEN (
          (
            LENGTH(sbcustname) + -1
          ) - 3
        ) <= 0
        THEN 0
        ELSE (
          LENGTH(sbcustname) + -1
        ) - 3
      END
    END
  ) AS wo_step6,
  SUBSTRING(
    sbcustname,
    CASE
      WHEN (
        LENGTH(sbcustname) + -3
      ) < 1
      THEN 1
      ELSE (
        LENGTH(sbcustname) + -3
      )
    END,
    CASE
      WHEN (
        3 - CASE
          WHEN (
            LENGTH(sbcustname) + -3
          ) < 1
          THEN 1
          ELSE (
            LENGTH(sbcustname) + -3
          )
        END
      ) <= 0
      THEN 0
      ELSE 3 - CASE
        WHEN (
          LENGTH(sbcustname) + -3
        ) < 1
        THEN 1
        ELSE (
          LENGTH(sbcustname) + -3
        )
      END
    END
  ) AS wo_step7,
  SUBSTRING(
    sbcustname,
    CASE
      WHEN (
        LENGTH(sbcustname) + -3
      ) < 1
      THEN 1
      ELSE (
        LENGTH(sbcustname) + -3
      )
    END,
    CASE
      WHEN (
        LENGTH(sbcustname) + -1
      ) < 1
      THEN 0
      ELSE (
        LENGTH(sbcustname) + -1
      ) - CASE
        WHEN (
          LENGTH(sbcustname) + -3
        ) < 1
        THEN 1
        ELSE (
          LENGTH(sbcustname) + -3
        )
      END
    END
  ) AS wo_step8,
  SUBSTRING(sbcustname, 3, 0) AS wo_step9
FROM main.sbcustomer
