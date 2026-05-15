WITH _s3 AS (
  SELECT
    1 AS n_rows,
    id
  FROM main.Dish
  WHERE
    LOWER(name) = 'baked apples with cream'
), _t1 AS (
  SELECT
    MenuPage.menu_id,
    MAX(MenuItem.price) AS max_price,
    SUM(_s3.n_rows) AS sum_n_rows
  FROM main.MenuPage AS MenuPage
  JOIN main.MenuItem AS MenuItem
    ON MenuItem.menu_page_id = MenuPage.id
  LEFT JOIN _s3 AS _s3
    ON MenuItem.dish_id = _s3.id
  GROUP BY
    1
)
SELECT
  Menu.sponsor
FROM main.Menu AS Menu
JOIN _t1 AS _t1
  ON Menu.id = _t1.menu_id AND _t1.sum_n_rows <> 0
ORDER BY
  _t1.max_price DESC
LIMIT 1
