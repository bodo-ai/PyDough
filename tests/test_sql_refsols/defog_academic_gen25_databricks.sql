SELECT DISTINCT
  author.name AS author_name
FROM defog.academic.author AS author
JOIN defog.academic.writes AS writes
  ON author.aid = writes.aid
JOIN defog.academic.domain_publication AS domain_publication
  ON domain_publication.pid = writes.pid
JOIN defog.academic.domain AS domain
  ON domain.did = domain_publication.did AND domain.name = 'Computer Science'
