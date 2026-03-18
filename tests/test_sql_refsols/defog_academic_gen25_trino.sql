SELECT DISTINCT
  author.name AS author_name
FROM postgres.author AS author
JOIN postgres.writes AS writes
  ON author.aid = writes.aid
JOIN postgres.domain_publication AS domain_publication
  ON domain_publication.pid = writes.pid
JOIN postgres.domain AS domain
  ON domain.did = domain_publication.did AND domain.name = 'Computer Science'
