WITH _t1 AS (
  SELECT
    n_name,
    n_nationkey
  FROM tpch.NATION
  WHERE
    n_name IN ('CANADA', 'ARGENTINA')
), _s6 AS (
  SELECT DISTINCT
    EXTRACT(YEAR FROM CAST(ORDERS.o_orderdate AS DATETIME)) AS year_o_orderdate,
    _t1.n_name
  FROM tpch.ORDERS AS ORDERS
  JOIN tpch.CUSTOMER AS CUSTOMER
    ON CUSTOMER.c_custkey = ORDERS.o_custkey
  JOIN _t1 AS _t1
    ON CUSTOMER.c_nationkey = _t1.n_nationkey
), _s7 AS (
  SELECT DISTINCT
    _t3.n_name
  FROM tpch.SUPPLIER AS SUPPLIER
  JOIN _t1 AS _t3
    ON SUPPLIER.s_nationkey = _t3.n_nationkey
)
SELECT
  _s6.n_name AS c_nation,
  _s6.year_o_orderdate AS o_year
FROM _s6 AS _s6
JOIN _s7 AS _s7
  ON _s6.n_name = _s7.n_name
