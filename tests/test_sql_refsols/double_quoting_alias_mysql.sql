SELECT
  COUNT(Album.`ArtistId`) AS n_albums
FROM main.Artist AS Artist
LEFT JOIN main.Album AS Album
  ON Album.`ArtistId` = Artist.`ArtistId`
WHERE
  Artist.Name = 'AC/DC'
GROUP BY
  Artist.`ArtistId`
