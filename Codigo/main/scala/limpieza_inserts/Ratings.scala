package limpieza_inserts

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}  // Librería para leer archivos CSV
import json_case_class.Rating  // Importa la clase case class para representar la calificación de usuario
import play.api.libs.json.*  // Librerías de Play Framework para trabajar con JSON
import utilidades.EscapeCaracteresEspeciales.limpiarJson  // Función personalizada para limpiar caracteres especiales en JSON

import java.io.{File, PrintWriter}  // Para manejar la escritura de archivos
import scala.collection.mutable.ListBuffer  // Para usar listas mutables durante el procesamiento
import scala.util.{Failure, Success, Try}  // Para manejar excepciones de manera funcional

object Ratings extends App {

  // Rutas para los archivos de entrada y salida
  val rutaCsv = "data/pi_movies_complete.csv"  // Archivo CSV de entrada
  val rutaArchivoSql = "data/inserts_ratings.sql"  // Archivo SQL de salida

  // Se define el formato del archivo CSV, en este caso el delimitador es punto y coma (;)
  implicit val csvFormat: DefaultCSVFormat = new DefaultCSVFormat {
    override val delimiter = ';'  // Configura el delimitador para el CSV
  }

  // Abre el archivo CSV para lectura y el archivo SQL para escritura
  val reader = CSVReader.open(new File(rutaCsv))
  val writer = new PrintWriter(new File(rutaArchivoSql))  // Abre el archivo de salida para escribir el SQL

  try {
    // Lee todas las filas del CSV y mantiene los encabezados
    val datos = reader.allWithHeaders()

    // Filtra las filas que no están vacías ni contienen solo "<unset>" en sus valores
    val datosFiltrados = datos.filter { fila =>
      val columnasExcluyendoId = fila - "id"  // Excluye la columna "id" de la validación
      columnasExcluyendoId.values.exists(_.trim.nonEmpty) &&  // Verifica que al menos una columna tenga datos no vacíos
        !columnasExcluyendoId.values.forall(_.trim.toLowerCase == "<unset>")  // Asegura que no todas las columnas tengan "<unset>"
    }.distinct  // Elimina filas duplicadas

    println(s"Total de filas después de filtrar: ${datosFiltrados.size}")  // Imprime el número de filas después del filtrado

    // Listas donde se almacenarán los usuarios y las relaciones entre películas, usuarios y sus calificaciones
    val userList = new scala.collection.mutable.HashSet[Int]()  // Conjunto para almacenar los IDs únicos de los usuarios
    val movieUserList = new ListBuffer[(Int, Int, Double, Long)]()  // Relación entre película, usuario, calificación y timestamp

    // Recorre cada fila del CSV filtrado
    datosFiltrados.foreach { fila =>
      // Intenta extraer el ID de la película y procesar el campo JSON de calificaciones
      for {
        movieId <- Try(fila("id").trim.toInt).toOption  // Obtiene el ID de la película
        jsonStr = fila.getOrElse("ratings", "").trim if jsonStr.nonEmpty && jsonStr != "\"\""  // Extrae y valida el JSON de calificaciones
        jsonLimpio = limpiarJson(jsonStr).replaceAll("'", "\"")  // Limpia el JSON y reemplaza comillas simples por dobles
        jsonArray <- Try(Json.parse(jsonLimpio).as[JsArray]).toOption  // Parsea el JSON limpio como un array de objetos
      } jsonArray.value.foreach { obj =>
        // Valida el JSON para asegurarse de que sea un Rating (calificación)
        obj.validate[Rating] match {
          case JsSuccess(rating, _) =>
            userList += rating.userId  // Añade el ID del usuario a la lista de usuarios
            movieUserList += ((movieId, rating.userId, rating.rating, rating.timestamp))  // Añade la relación película-usuario-calificación-timestamp a la lista
          case JsError(errors) =>
            println(s"Error en el JSON de ratings: $errors")  // Maneja errores de validación en el JSON de calificación
        }
      }
    }

    try {
      // Escribe los inserts para la tabla 'user' (usuarios) en el archivo SQL
      writer.println("-- Inserts para la tabla user")
      userList.foreach { userId =>
        writer.println(s"INSERT INTO user (userid) VALUES ($userId);")  // Inserta cada ID de usuario en la tabla 'user'
      }

      // Escribe los inserts para la tabla 'movieUser' (relación entre películas, usuarios y calificaciones) en el archivo SQL
      writer.println("\n-- Inserts para la tabla movieUser")
      movieUserList.distinct.foreach { case (movieId, userId, rating, timestamp) =>
        writer.println(s"INSERT INTO movieUser (movie_id, userid, rating, timestamp1) VALUES ($movieId, $userId, $rating, $timestamp);")
        // Inserta la relación entre película, usuario, calificación y timestamp en la tabla 'movieUser'
      }

      println("Archivo SQL generado exitosamente: inserts_ratings.sql")  // Mensaje de éxito

    } catch {
      case e: Exception => println(s"Error al escribir el archivo SQL: ${e.getMessage}")  // Manejo de errores al escribir el archivo SQL
    } finally {
      writer.close()  // Cierra el escritor al finalizar
    }

  } catch {
    case e: Exception => println(s"Error crítico: ${e.getMessage}")  // Manejo de errores críticos al leer el archivo CSV
  } finally {
    reader.close()  // Cierra el lector al finalizar
  }
}
