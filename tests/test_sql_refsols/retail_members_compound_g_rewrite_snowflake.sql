SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  date_of_birth IN ('1671-07-20', '2830-05-15', '1615-07-24', '1330-12-30', '2238-04-21', '0816-01-07', '2121-12-17')
