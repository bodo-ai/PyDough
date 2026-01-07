SELECT DISTINCT
  author.name AS author_name
FROM main.author AS author
JOIN main.writes AS writes
  ON author.aid = writes.aid
JOIN main.domain_publication AS domain_publication
  ON domain_publication.pid = writes.pid
JOIN main.domain AS domain
  ON domain.did = domain_publication.did AND domain.name = 'Computer Science'
