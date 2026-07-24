WITH _s3 AS (
  SELECT
    1 AS n_rows,
    id
  FROM main.dish
  WHERE
    LOWER(name) = 'baked apples with cream'
), _t1 AS (
  SELECT
    menupage.menu_id,
    MAX(menuitem.price) AS max_price,
    SUM(_s3.n_rows) AS sum_n_rows
  FROM main.menupage AS menupage
  JOIN main.menuitem AS menuitem
    ON menuitem.menu_page_id = menupage.id
  LEFT JOIN _s3 AS _s3
    ON _s3.id = menuitem.dish_id
  GROUP BY
    1
)
SELECT
  menu.sponsor
FROM main.menu AS menu
JOIN _t1 AS _t1
  ON _t1.menu_id = menu.id AND _t1.sum_n_rows <> 0
ORDER BY
  _t1.max_price DESC
LIMIT 1
