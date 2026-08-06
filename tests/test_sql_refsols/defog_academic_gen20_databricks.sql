SELECT
  COUNT(*) AS n
FROM defog.academic.publication AS publication
JOIN defog.academic.journal AS journal
  ON STARTSWITH(LOWER(journal.name), 'j') AND journal.jid = publication.jid
