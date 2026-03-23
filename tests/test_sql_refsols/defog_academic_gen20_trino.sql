SELECT
  COUNT(*) AS n
FROM postgres.main.publication AS publication
JOIN postgres.main.journal AS journal
  ON STARTS_WITH(LOWER(journal.name), 'j') AND journal.jid = publication.jid
