WITH _table_alias_0 AS (
  SELECT
    wallet_transactions_daily.amount AS amount,
    wallet_transactions_daily.sender_id AS sender_id
  FROM main.wallet_transactions_daily AS wallet_transactions_daily
  WHERE
    wallet_transactions_daily.sender_type = 0
), _table_alias_1 AS (
  SELECT
    users.country AS country,
    users.uid AS uid
  FROM main.users AS users
), _t2 AS (
  SELECT
    COUNT(DISTINCT _table_alias_0.sender_id) AS agg_1,
    SUM(_table_alias_0.amount) AS agg_0,
    _table_alias_1.country AS country
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.sender_id = _table_alias_1.uid
  GROUP BY
    _table_alias_1.country
), _t0 AS (
  SELECT
    _t2.country AS country,
    COALESCE(_t2.agg_0, 0) AS ordering_2,
    COALESCE(_t2.agg_0, 0) AS total_amount,
    _t2.agg_1 AS user_count
  FROM _t2 AS _t2
  ORDER BY
    ordering_2 DESC
  LIMIT 5
)
SELECT
  _t0.country AS country,
  _t0.user_count AS user_count,
  _t0.total_amount AS total_amount
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_2 DESC
