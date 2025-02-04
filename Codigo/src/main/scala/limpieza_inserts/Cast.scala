package limpieza_inserts

// Se importan las bibliotecas necesarias para manejar archivos CSV, JSON, y para escritura de archivos.
import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import json_case_class.Cast
import play.api.libs.json.*
import utilidades.EscapeCaracteresEspeciales.limpiarJsonCrew

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

// El objeto Cast extiende la aplicación principal.
object Cast extends App {
  // Definición de las rutas de los archivos de entrada y salida.
  val rutaCsv = "data/pi_movies_complete.csv"  // Ruta del archivo CSV de entrada.
  val rutaArchivoSql = "data/inserts_cast.sql"  // Ruta del archivo SQL de salida.

  // Definición de un formato personalizado para el CSV donde se utiliza el punto y coma (;) como delimitador.
  implicit val csvFormat: DefaultCSVFormat = new DefaultCSVFormat {
    override val delimiter = ';'  // El delimitador entre los campos en el CSV es el punto y coma.
  }

  // Se abren los archivos de entrada (CSV) y salida (SQL).
  val reader = CSVReader.open(new File(rutaCsv))  // Abrir archivo CSV.
  val writer = new PrintWriter(new File(rutaArchivoSql))  // Abrir archivo de salida SQL.

  try {
    // Se lee todo el contenido del archivo CSV y se obtiene una lista de filas con encabezados.
    val datos = reader.allWithHeaders()

    // Se filtran las filas para eliminar aquellas que tengan todos los campos vacíos o con el valor "<unset>".
    val datosFiltrados = datos.filter { fila =>
      val columnasExcluyendoId = fila - "id"  // Se elimina la columna "id" del procesamiento.
      // Se verifica si al menos una de las columnas tiene un valor no vacío o no igual a "<unset>".
      columnasExcluyendoId.values.exists(_.trim.nonEmpty) &&
        !columnasExcluyendoId.values.forall(_.trim.toLowerCase == "<unset>")
    }.distinct

    // Se imprime el número total de filas después del filtrado.
    println(s"Total de filas después de filtrar: ${datosFiltrados.size}")

    // Se inicializan las listas para almacenar los datos de los actores y las relaciones de películas con actores.
    val actorsList = new ListBuffer[Cast]()  // Lista para almacenar actores.
    val movieActorsList = new ListBuffer[(Int, Int, String, Int, Int, String)]()  // Lista para almacenar relaciones película-actor.

    // Se procesa cada fila del archivo CSV filtrado.
    datosFiltrados.foreach { fila =>
      for {
        movieId <- Try(fila("id").trim.toInt).toOption  // Se obtiene el ID de la película.
        jsonStr = fila.getOrElse("cast", "").trim if jsonStr.nonEmpty && jsonStr != "\"\""  // Se obtiene el JSON con los datos del elenco.
        // Se limpia el JSON eliminando caracteres especiales.
        jsonLimpio = limpiarJsonCrew(jsonStr).replaceAll("'", "\"")
        jsonArray <- Try(Json.parse(jsonLimpio).as[List[JsObject]]).toOption  // Se parsea el JSON a un array de objetos.
      } jsonArray.foreach { jsonObj =>
        jsonObj.validate[Cast] match {  // Se valida el objeto JSON como un tipo Cast.
          case JsSuccess(cast, _) =>  // Si la validación es exitosa.
            actorsList += cast  // Se agrega el actor a la lista de actores.
            movieActorsList += ((movieId, cast.id, cast.character, cast.order, cast.cast_id, cast.credit_id))  // Se agrega la relación película-actor.
          case JsError(errors) =>  // Si la validación falla.
            println(s"Error en el cast JSON: $errors")  // Se imprime el error de validación.
        }
      }
    }

    // Intento de escribir las inserciones SQL en el archivo de salida.
    try {
      // Se escribe un comentario en el archivo indicando que se insertarán actores.
      writer.println("-- Inserts para la tabla actors")
      // Se agrupan los actores por su ID y se genera un INSERT para cada actor.
      actorsList.groupBy(_.id).foreach { case (idActor, castList) =>
        val cast = castList.head
        val nameEscaped = cast.name.replace("'", "''")  // Se escapa el nombre del actor para evitar errores en el SQL.
        val profilePathEscaped = Option(cast.profile_path).getOrElse("").replace("'", "''")  // Se escapa la ruta del perfil.
        val gender = cast.gender  // Se obtiene el género del actor.

        // Se genera y escribe la sentencia SQL para insertar el actor.
        writer.println(
          s"INSERT INTO actors (id_actors, actors_name, gender, profile_path) VALUES ($idActor, '$nameEscaped', $gender, '$profilePathEscaped');"
        )
      }

      // Se escribe un comentario en el archivo indicando que se insertarán relaciones película-actor.
      writer.println("\n-- Inserts para la tabla movieActors")
      // Se generan las sentencias SQL para insertar las relaciones película-actor.
      movieActorsList.distinct.foreach { relation =>
        writer.println(s"INSERT INTO movieActors (movie_id, id_actors, character1, order1, cast_id, credit_id) VALUES (${relation._1}, ${relation._2}, '${relation._3}', ${relation._4}, ${relation._5}, '${relation._6}');")
      }

      // Se imprime un mensaje indicando que el archivo SQL se generó exitosamente.
      println("Archivo SQL generado exitosamente: inserts_cast.sql")
    } catch {
      // Si ocurre un error al escribir el archivo SQL, se captura y muestra un mensaje de error.
      case e: Exception => println(s"Error al escribir el archivo SQL: ${e.getMessage}")
    } finally {
      // Se cierra el escritor del archivo SQL.
      writer.close()
    }

  } catch {
    // Si ocurre un error crítico, se captura y se imprime un mensaje de error.
    case e: Exception => println(s"Error crítico: ${e.getMessage}")
  } finally {
    // Se cierra el lector del archivo CSV.
    reader.close()
  }
}
