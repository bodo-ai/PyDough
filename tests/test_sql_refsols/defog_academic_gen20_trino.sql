SELECT
  COUNT(*) AS n
FROM postgres.publication AS publication
JOIN postgres.journal AS journal
  ON STARTS_WITH(LOWER(journal.name), 'j') AND journal.jid = publication.jid
