package sql_codigo

import cats.effect.{IO, IOApp}  // Importa IO y IOApp para manejar efectos y aplicaciones asíncronas
import doobie._  // Librería Doobie para manejar conexiones a bases de datos con JDBC
import doobie.implicits._  // Implicaciones de Doobie para escribir SQL de forma más sencilla
import utilidades.Connector.xa  // Conexión a la base de datos proporcionada por el objeto Connector
import cats.effect.{IO, IOApp}  // Librería Cats Effect para manejar efectos asincrónicos y de recursos

object Consultas extends IOApp.Simple {

  // Función para insertar los registros
  def insertarDatos(): IO[Unit] = {
    val insertSQL =
      sql"""
      INSERT INTO languages (iso_639_1, languages_name) VALUES ('zv', 'Zamoviano');
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO movie (movie_id, adult, budget, homepage, imdb_id, original_language, original_title, overview, popularity, poster_path, release_date, revenue,
       runtime, status, tagline, title, video, vote_average, vote_count)
      VALUES (1, 'false', 160000000, 'https://www.warnerbros.com/movies/inception', 'tt1375666', 'en', 'Inception', 'A thief who enters the dreams of others...', 91.2,
       '/poster.jpg', '2010-07-16', 829895144, 148, 'Released', 'Your mind is the scene of the crime.', 'Inception', 'false', 8.8, 2000000);
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO belongs_to_collection (collection_name, collection_backdrop_path, collection_poster_path, movieCollection)
      VALUES ('Inception Collection', '/backdrop.jpg', '/collection_poster.jpg', 1);
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO genres (genres_name) VALUES ('Action'), ('Sci-Fi'), ('Adventure');
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO movieGenres (movie_id, genre_id) VALUES (1, 28), (1, 10772), (1, 10773);
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO user (userid) VALUES (99999999);
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO countries (iso_3166_1, countries_name) VALUES ('EC', 'Ecuador');
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO movieCountries (movie_id, iso_3166_1) VALUES (1, 'EC');
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO company (company_name) VALUES ('Warner Bros. Entertainment'), ('Legendary Pictures');
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO movieCompanies (movie_id, id_company) VALUES (1, 17), (1, 923);
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO actors (id_actors, actors_name, gender, profile_path) VALUES
      (1907175,'Leonardo DiCaprio', 1, '/leo.jpg'),
      (1907176,'Joseph Gordon-Levitt', 2, '/joseph.jpg'),
      (1907177,'Elliot Page', 2, '/elliot.jpg');
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO movieActors (movie_id, id_actors, character1, order1, cast_id, credit_id) VALUES
      (1, 1907175, 'Patrick', 4, 7, 'abc123'),
      (1, 1907176, 'Arthur', 2, 102, 'abc124'),
      (1, 1907177, 'Ariadne', 3, 103, 'abc125');
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO personnel (id_personnel, personnel_name, gender, profile_path) VALUES
      (1907331,'Christopher Nolan', 1, '/nolan.jpg'),
      (1907332,'Hans Zimmer', 1, '/zimmer.jpg');
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO department (id_department, department_name) VALUES (13, 'Players'), (14, 'Filming');
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO job (id_job, job_name, id_department) VALUES
      (348, 'Franty', 1),
      (349, 'Fanty', 2);
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO personnelJob (id_personnel, id_job) VALUES (1907331, 1), (1907332, 2);
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO moviePersonnel (movie_id, id_personnel, job, department, credit_id) VALUES
        (1, 1907331, 'Director', 'Directing', 'nolan_dir'),
      (1, 1907332, 'Composer', 'Music', 'zimmer_music');
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO keywords (id_keywords, keywords_name) VALUES (238816, 'panto'), (238817, 'mima'), (238818, 'morcilla');
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO movieKeywords (movie_id, id_keywords) VALUES (1, 238816), (1, 238817), (1, 238818);
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO movieUser (movie_id, userid, rating, timestamp1) VALUES (1, 99999999, 9.5, 1625097600);
    """.update.run.transact(xa) *>
        sql"""
      INSERT INTO movieLanguages (movie_id, iso_639_1) VALUES (1, 'zv');
    """.update.run.transact(xa).map(_ => ())



    insertSQL // Ejecuta las inserciones
  }

  // 1. Consultar las 5 películas con más actores
  def obtenerPeliculasConMasActores(): IO[List[(Int, String, Int)]] = {
    val query = sql"""
      SELECT m.movie_id, m.title, COUNT(ma.id_actors) AS total_actors
      FROM movieActors ma
      JOIN movie m ON ma.movie_id = m.movie_id
      GROUP BY m.movie_id, m.title
      ORDER BY total_actors DESC
      LIMIT 5;
    """.query[(Int, String, Int)].to[List]
    query.transact(xa)  // Ejecuta la consulta y devuelve el resultado como una lista de tuplas
  }

  // 2. Consultar las películas cuyo revenue es mayor al promedio de todas las películas
  def obtenerPeliculasConMayorRevenue(): IO[List[(Int, String, Double)]] = {
    val query = sql"""
        SELECT movie_id, title, revenue
        FROM movie
        WHERE revenue > (SELECT AVG(revenue) FROM movie)
        ORDER BY revenue DESC;
    """.query[(Int, String, Double)].to[List]
    query.transact(xa)  // Ejecuta la consulta y devuelve las películas con revenue mayor al promedio
  }

  // 3. Consultar las películas que tienen más de 5 géneros asociados
  def obtenerPeliculasConMasDe5Generos(): IO[List[(Int, String, Int)]] = {
    val query = sql"""
      SELECT m.movie_id, m.title, COUNT(mg.genre_id) AS total_genres
      FROM movieGenres mg
      JOIN movie m ON mg.movie_id = m.movie_id
      GROUP BY m.movie_id, m.title
      HAVING total_genres > 5;
    """.query[(Int, String, Int)].to[List]
    query.transact(xa)  // Ejecuta la consulta y devuelve las películas con más de 5 géneros
  }

  // 4. Consultar los actores con más películas asociadas
  def obtenerActoresConMasPeliculas(): IO[List[(String, Int)]] = {
    val query = sql"""
      SELECT a.actors_name, COUNT(ma.movie_id) AS total_movies
      FROM movieActors ma
      JOIN actors a ON ma.id_actors = a.id_actors
      GROUP BY a.actors_name
      ORDER BY total_movies DESC
      LIMIT 5;
    """.query[(String, Int)].to[List]
    query.transact(xa)  // Ejecuta la consulta y devuelve los actores con más películas
  }

  // 5. Consultar el país con más películas asociadas
  def obtenerPaisConMasPeliculas(): IO[List[(String, Int)]] = {
    val query = sql"""
      SELECT c.countries_name, COUNT(mc.movie_id) AS total_movies
      FROM movieCountries mc
      JOIN countries c ON mc.iso_3166_1 = c.iso_3166_1
      GROUP BY c.countries_name
      ORDER BY total_movies DESC
      LIMIT 1;
    """.query[(String, Int)].to[List]
    query.transact(xa)  // Ejecuta la consulta y devuelve el país con más películas
  }

  // 6. Consultar los usuarios con más reseñas y su promedio de calificación
  def obtenerUsuariosConMasReseñas(): IO[List[(Int, Int, Double)]] = {
    val query = sql"""
      SELECT u.userid, COUNT(mu.movie_id) AS total_reviews, AVG(mu.rating) AS avg_rating
      FROM movieUser mu
      JOIN user u ON mu.userid = u.userid
      GROUP BY u.userid
      ORDER BY total_reviews DESC
      LIMIT 5;
    """.query[(Int, Int, Double)].to[List]
    query.transact(xa)  // Ejecuta la consulta y devuelve los usuarios con más reseñas
  }

  // 7. Consultar las películas con mayor revenue por minuto
  def obtenerPeliculasConMayorRevenuePorMinuto(): IO[List[(Int, String, Double)]] = {
    val query = sql"""
      SELECT movie_id, title, (revenue / runtime) AS revenue_per_minute
      FROM movie
      WHERE runtime > 0
      ORDER BY revenue_per_minute DESC
      LIMIT 5;
    """.query[(Int, String, Double)].to[List]
    query.transact(xa)  // Ejecuta la consulta y devuelve las películas con mayor revenue por minuto
  }

  // 8. Consultar las películas con más personal involucrado en su producción
  def obtenerPeliculasConMasPersonal(): IO[List[(Int, String, Int)]] = {
    val query = sql"""
      SELECT m.movie_id, m.title, COUNT(mp.id_personnel) AS total_personnel
      FROM moviePersonnel mp
      JOIN movie m ON mp.movie_id = m.movie_id
      GROUP BY m.movie_id, m.title
      ORDER BY total_personnel DESC
      LIMIT 5;
    """.query[(Int, String, Int)].to[List]
    query.transact(xa)  // Ejecuta la consulta y devuelve las películas con más personal involucrado
  }
  // 9. Eliminar el código 'zv' de la columna iso_639_1 en la tabla languages
  def eliminarCodigoZV(): IO[Unit] = {
    val query =
      sql"""
      DELETE FROM languages WHERE iso_639_1 = 'zv';
    """.update.run.transact(xa).map(_ => ())

    query // Ejecuta la consulta
  }

  // Ejecutar las inserciones y las consultas
  def run: IO[Unit] = {
    for {
      peliculasMasActores <- obtenerPeliculasConMasActores()
      _ = println(s"Películas con más actores: $peliculasMasActores")

      peliculasMayorRevenue <- obtenerPeliculasConMayorRevenue()
      _ = println(s"Películas con mayor revenue: $peliculasMayorRevenue")

      peliculasMasDe5Generos <- obtenerPeliculasConMasDe5Generos()
      _ = println(s"Películas con más de 5 géneros: $peliculasMasDe5Generos")

      actoresMasPeliculas <- obtenerActoresConMasPeliculas()
      _ = println(s"Actores con más películas: $actoresMasPeliculas")

      paisMasPeliculas <- obtenerPaisConMasPeliculas()
      _ = println(s"País con más películas: $paisMasPeliculas")

      usuariosMasReseñas <- obtenerUsuariosConMasReseñas()
      _ = println(s"Usuarios con más reseñas: $usuariosMasReseñas")

      peliculasMayorRevenuePorMinuto <- obtenerPeliculasConMayorRevenuePorMinuto()
      _ = println(s"Películas con mayor revenue por minuto: $peliculasMayorRevenuePorMinuto")

      peliculasMasPersonal <- obtenerPeliculasConMasPersonal()
      _ = println(s"Películas con más personal involucrado: $peliculasMasPersonal")

      _ <- insertarDatos()  // Ejecuta las inserciones
      _ <- IO(println("Datos insertados correctamente"))

      _ <- eliminarCodigoZV()  // Ejecuta la eliminación del código 'zv'
      _ <- IO(println("Código 'zv' eliminado correctamente de la tabla languages"))

    } yield ()  // Imprime los resultados de las consultas
  }
}
