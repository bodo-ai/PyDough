WITH _t1 AS (
  SELECT
    n_name,
    n_nationkey
  FROM tpch.nation
  WHERE
    n_name IN ('CANADA', 'ARGENTINA')
), _s6 AS (
  SELECT DISTINCT
    EXTRACT(YEAR FROM CAST(orders.o_orderdate AS TIMESTAMP)) AS year_o_orderdate,
    _t1.n_name
  FROM tpch.orders AS orders
  JOIN tpch.customer AS customer
    ON customer.c_custkey = orders.o_custkey
  JOIN _t1 AS _t1
    ON _t1.n_nationkey = customer.c_nationkey
), _s7 AS (
  SELECT DISTINCT
    _t3.n_name
  FROM tpch.supplier AS supplier
  JOIN _t1 AS _t3
    ON _t3.n_nationkey = supplier.s_nationkey
)
SELECT
  _s6.n_name AS c_nation,
  _s6.year_o_orderdate AS o_year
FROM _s6 AS _s6
JOIN _s7 AS _s7
  ON _s6.n_name = _s7.n_name
