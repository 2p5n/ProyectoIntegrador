package limpieza_inserts

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import json_case_class.Genre
import json_cc_modelo_logico.MovieGenre
import play.api.libs.json._
import utilidades.EscapeCaracteresEspeciales.limpiarJson

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object Genres extends App {
  // Definición de las rutas de los archivos CSV y SQL
  val rutaCsv = "data/pi_movies_complete.csv" // Ruta al archivo CSV de películas
  val rutaArchivoSql = "data/insertsGenres.sql" // Ruta al archivo SQL de salida

  // Configuración del delimitador CSV
  implicit val csvFormat: DefaultCSVFormat = new DefaultCSVFormat {
    override val delimiter = ';' // El delimitador es punto y coma
  }

  // Abrir los archivos CSV y SQL
  val reader = CSVReader.open(new File(rutaCsv))
  val writer = new PrintWriter(new File(rutaArchivoSql))

  try {
    // Leer todos los datos del archivo CSV
    val datos = reader.allWithHeaders()

    // Filtrar las filas que contienen datos relevantes
    val datosFiltrados = datos.filter { fila =>
      val columnasExcluyendoId = fila - "id" // Excluir la columna "id"
      // Filtrar filas con valores no vacíos y que no contengan "<unset>"
      columnasExcluyendoId.values.exists(_.trim.nonEmpty) &&
        !columnasExcluyendoId.values.forall(_.trim.toLowerCase == "<unset>")
    }.distinct // Eliminar filas duplicadas

    println(s"Total de filas después de filtrar: ${datosFiltrados.size}")

    // Listas para almacenar los géneros y relaciones película-género
    val genresList = new ListBuffer[Genre]() // Almacena los géneros únicos
    val movieGenresList = new ListBuffer[MovieGenre]() // Almacena las relaciones entre películas y géneros

    // Procesar cada fila del CSV
    datosFiltrados.foreach { fila =>
      for {
        movieId <- Try(fila("id").trim.toInt).toOption // Obtener el ID de la película de forma segura
        jsonStr = fila.getOrElse("genres", "").trim if jsonStr.nonEmpty && jsonStr != "\"\"" // Validar y limpiar el JSON
        jsonLimpio = limpiarJson(jsonStr).replaceAll("'", "\"") // Limpiar y reemplazar comillas simples por dobles
        jsonArray <- Try(Json.parse(jsonLimpio).as[List[JsObject]]).toOption // Parsear el JSON a una lista de objetos
      } jsonArray.foreach { jsonObj =>
        // Validar y convertir cada objeto JSON en una instancia de Genre
        jsonObj.validate[Genre] match {
          case JsSuccess(genre, _) =>
            // Si la validación es exitosa, agregar el género a la lista
            genresList += genre
            // Crear la relación entre la película y el género
            movieGenresList += MovieGenre(movieId, genre.id)
          case JsError(errors) =>
            // Si ocurre un error al validar el JSON, imprimir los errores
            println(s"Error en el JSON de género: $errors")
        }
      }
    }

    // Generar el archivo SQL con las inserciones
    try {
      // Inserciones para la tabla genres (géneros)
      writer.println("-- Inserts para la tabla genres")
      genresList.groupBy(_.id).foreach { case (idGenre, genreList) =>
        val genre = genreList.head // Obtener el primer género para este id (los géneros deben ser únicos por id)
        val nameEscaped = genre.name.replace("'", "''") // Escapar las comillas simples en el nombre del género
        // Insertar el género en la tabla genres
        writer.println(
          s"INSERT INTO genres (id, genres_name) VALUES ($idGenre, '$nameEscaped');"
        )
      }

      // Inserciones para la tabla movieGenres (películas-géneros)
      writer.println("\n-- Inserts para la tabla movieGenres")
      movieGenresList.distinct.foreach { relation =>
        // Insertar la relación entre películas y géneros
        writer.println(s"INSERT INTO movieGenres (movie_id, genre_id) VALUES (${relation.movieId}, ${relation.genreId});")
      }

      println("Archivo SQL generado exitosamente: insertsGenres.sql")
    } catch {
      case e: Exception => println(s"Error al escribir el archivo SQL: ${e.getMessage}") // Manejar errores de escritura
    } finally {
      writer.close() // Cerrar el archivo SQL después de escribir
    }

  } catch {
    case e: Exception => println(s"Error crítico: ${e.getMessage}") // Manejar errores generales
  } finally {
    reader.close() // Cerrar el archivo CSV después de leer
  }
}
