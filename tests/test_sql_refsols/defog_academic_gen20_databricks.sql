SELECT
  COUNT(*) AS n
FROM main.publication AS publication
JOIN main.journal AS journal
  ON STARTSWITH(LOWER(journal.name), 'j') AND journal.jid = publication.jid
