SELECT DISTINCT
  author.name AS author_name
FROM academic.author AS author
JOIN academic.writes AS writes
  ON author.aid = writes.aid
JOIN academic.domain_publication AS domain_publication
  ON domain_publication.pid = writes.pid
JOIN academic.domain AS domain
  ON domain.did = domain_publication.did AND domain.name = 'Computer Science'
