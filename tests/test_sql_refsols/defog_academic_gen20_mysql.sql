SELECT
  COUNT(*) AS n
FROM publication AS publication
JOIN journal AS journal
  ON LOWER(journal.name) LIKE 'j%' AND journal.jid = publication.jid
