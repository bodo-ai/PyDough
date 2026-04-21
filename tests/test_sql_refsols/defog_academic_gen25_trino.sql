SELECT DISTINCT
  author.name AS author_name
FROM postgres.main.author AS author
JOIN cassandra.defog.writes AS writes
  ON author.aid = writes.aid
JOIN cassandra.defog.domain_publication AS domain_publication
  ON domain_publication.pid = writes.pid
JOIN postgres.main.domain AS domain
  ON domain.did = domain_publication.did AND domain.name = 'Computer Science'
