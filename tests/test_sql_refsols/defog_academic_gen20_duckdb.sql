SELECT
  COUNT(*) AS n
FROM main.publication AS publication
JOIN main.journal AS journal
  ON LOWER(journal.name) LIKE 'j%' AND journal.jid = publication.jid
