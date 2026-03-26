SELECT
  sbcustname AS original_name,
  RPAD('Cust0001', 30, '*') AS ref_rpad,
  LPAD('Cust0001', 30, '*') AS ref_lpad,
  RPAD(sbcustname, 30, '*') AS right_padded,
  LPAD(sbcustname, 30, '#') AS left_padded,
  RPAD(sbcustname, 8, '-') AS truncated_right,
  LPAD(sbcustname, 8, '-') AS truncated_left,
  RPAD(sbcustname, 0, '.') AS zero_pad_right,
  LPAD(sbcustname, 0, '.') AS zero_pad_left,
  RPAD(sbcustname, 30, ' ') AS right_padded_space,
  LPAD(sbcustname, 30, ' ') AS left_padded_space
FROM main.sbcustomer
ORDER BY
  1 NULLS FIRST
LIMIT 5
