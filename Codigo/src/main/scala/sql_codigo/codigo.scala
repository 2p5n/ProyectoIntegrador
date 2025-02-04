package sql_codigo

import doobie._  // Librería para interactuar con bases de datos usando JDBC
import doobie.implicits._  // Implicaciones de Doobie para trabajar con SQL y operaciones de base de datos

import cats.effect.unsafe.implicits.global  // Implicaciones de Cats para efectos asincrónicos, se usa el contexto de ejecución global
import cats.effect._  // Librerías de Cats Effect para manejar efectos asincrónicos

import cats.implicits._  // Para usar las implicaciones de Cats para trabajar con listas y otros coleccionables

import movie_case_class._  // Importa la clase case class de película para representar las filas de la tabla 'movie'
import utilidades.Connector  // Importa la utilidad de conexión para manejar la conexión a la base de datos

object codigo {

  // Definición de la función para insertar una película en la tabla 'movie'
  def insert(est: movie_table): ConnectionIO[Int] = {
    sql"""
       INSERT INTO movie (id, adult, budget, homepage, imdb_id,
       original_language, original_title, overview, popularity, poster_path,
       release_date, revenue, runtime, status, tagline, title, video, vote_average, vote_count)
       VALUES (
         ${est.id},  // Inserta el ID de la película
         ${est.adult},  // Inserta el valor de 'adult' (si es película para adultos)
         ${est.budget},  // Inserta el presupuesto de la película
         ${est.homepage},  // Inserta la URL de la página web de la película
         ${est.imdb_id},  // Inserta el ID de IMDb de la película
         ${est.original_language},  // Inserta el idioma original de la película
         ${est.original_title},  // Inserta el título original de la película
         ${est.overview},  // Inserta la descripción general de la película
         ${est.popularity},  // Inserta la popularidad de la película
         ${est.poster_path},  // Inserta la URL del póster de la película
         CASE WHEN ${est.release_date} = 'NULL' THEN NULL ELSE ${est.release_date} END,  // Condicional para manejar fechas nulas
         ${est.revenue},  // Inserta los ingresos de la película
         ${est.runtime},  // Inserta la duración de la película en minutos
         CASE WHEN LENGTH(${est.status}) > 50 THEN NULL ELSE ${est.status} END,  // Limita la longitud del 'status' a 50 caracteres
         ${est.tagline},  // Inserta la frase promocional de la película
         ${est.title},  // Inserta el título de la película
         ${est.video},  // Inserta si la película tiene un video disponible
         ${est.vote_average},  // Inserta el promedio de votos
         ${est.vote_count}  // Inserta el número de votos
       )
     """.update.run  // Ejecuta la consulta SQL de inserción y devuelve el número de filas afectadas
  }

  // Función para insertar una lista de objetos 'movie_table' en la base de datos
  def insertAll(est: List[movie_table]): IO[List[Int]] = {
    // Usa 'traverse' para aplicar la función 'insert' a cada elemento de la lista de películas y ejecutarlo de forma asincrónica
    est.traverse(t => insert(t).transact(Connector.xa))  // 'transact' asegura que las operaciones se ejecuten dentro de una transacción
  }
}
