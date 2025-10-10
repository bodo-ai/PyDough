WITH _s1 AS (
  SELECT
    jid,
    COUNT(*) AS n_rows
  FROM main.publication
  GROUP BY
    1
)
SELECT
  journal.name,
  journal.jid,
  COALESCE(_s1.n_rows, 0) AS num_publications
FROM main.journal AS journal
LEFT JOIN _s1 AS _s1
  ON _s1.jid = journal.jid
ORDER BY
  3 DESC
