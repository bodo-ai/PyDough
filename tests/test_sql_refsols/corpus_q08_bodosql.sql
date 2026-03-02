WITH _t1 AS (
  SELECT
    word
  FROM dict
  WHERE
    CONTAINS(word, 'aa')
    OR CONTAINS(word, 'bb')
    OR CONTAINS(word, 'cc')
    OR CONTAINS(word, 'dd')
    OR CONTAINS(word, 'ee')
    OR CONTAINS(word, 'ff')
    OR CONTAINS(word, 'gg')
    OR CONTAINS(word, 'hh')
    OR CONTAINS(word, 'ii')
    OR CONTAINS(word, 'jj')
    OR CONTAINS(word, 'kk')
    OR CONTAINS(word, 'll')
    OR CONTAINS(word, 'mm')
    OR CONTAINS(word, 'nn')
    OR CONTAINS(word, 'oo')
    OR CONTAINS(word, 'pp')
    OR CONTAINS(word, 'qq')
    OR CONTAINS(word, 'rr')
    OR CONTAINS(word, 'ss')
    OR CONTAINS(word, 'tt')
    OR CONTAINS(word, 'uu')
    OR CONTAINS(word, 'vv')
    OR CONTAINS(word, 'ww')
    OR CONTAINS(word, 'xx')
    OR CONTAINS(word, 'yy')
    OR CONTAINS(word, 'zz')
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY word ORDER BY pos) = 1
)
SELECT
  COUNT_IF(CONTAINS(word, 'aa')) AS n_aa,
  COUNT_IF(CONTAINS(word, 'bb')) AS n_bb,
  COUNT_IF(CONTAINS(word, 'cc')) AS n_cc,
  COUNT_IF(CONTAINS(word, 'dd')) AS n_dd,
  COUNT_IF(CONTAINS(word, 'ee')) AS n_ee,
  COUNT_IF(CONTAINS(word, 'ff')) AS n_ff,
  COUNT_IF(CONTAINS(word, 'gg')) AS n_gg,
  COUNT_IF(CONTAINS(word, 'hh')) AS n_hh,
  COUNT_IF(CONTAINS(word, 'ii')) AS n_ii,
  COUNT_IF(CONTAINS(word, 'jj')) AS n_jj,
  COUNT_IF(CONTAINS(word, 'kk')) AS n_kk,
  COUNT_IF(CONTAINS(word, 'll')) AS n_ll,
  COUNT_IF(CONTAINS(word, 'mm')) AS n_mm,
  COUNT_IF(CONTAINS(word, 'nn')) AS n_nn,
  COUNT_IF(CONTAINS(word, 'oo')) AS n_oo,
  COUNT_IF(CONTAINS(word, 'pp')) AS n_pp,
  COUNT_IF(CONTAINS(word, 'qq')) AS n_qq,
  COUNT_IF(CONTAINS(word, 'rr')) AS n_rr,
  COUNT_IF(CONTAINS(word, 'ss')) AS n_ss,
  COUNT_IF(CONTAINS(word, 'tt')) AS n_tt,
  COUNT_IF(CONTAINS(word, 'uu')) AS n_uu,
  COUNT_IF(CONTAINS(word, 'vv')) AS n_vv,
  COUNT_IF(CONTAINS(word, 'ww')) AS n_ww,
  COUNT_IF(CONTAINS(word, 'xx')) AS n_xx,
  COUNT_IF(CONTAINS(word, 'yy')) AS n_yy,
  COUNT_IF(CONTAINS(word, 'zz')) AS n_zz
FROM _t1
