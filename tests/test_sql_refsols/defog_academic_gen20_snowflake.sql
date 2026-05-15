SELECT
  COUNT(*) AS n
FROM academic.publication AS publication
JOIN academic.journal AS journal
  ON STARTSWITH(LOWER(journal.name), 'j') AND journal.jid = publication.jid
