package limpieza_inserts

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import json_case_class.Keyword
import play.api.libs.json._
import utilidades.EscapeCaracteresEspeciales.limpiarJson

import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

object Keywords extends App {
  // Definición de las rutas de los archivos CSV y SQL
  val rutaCsv = "data/pi_movies_complete.csv" // Ruta al archivo CSV de películas
  val rutaArchivoSql = "data/insertsKeywords.sql" // Ruta al archivo SQL de salida

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

    // Listas para almacenar las palabras clave y relaciones película-palabra clave
    val keywordsList = new ListBuffer[Keyword]() // Almacena las palabras clave únicas
    val movieKeywordsList = new ListBuffer[(Int, Int)]() // Almacena las relaciones entre películas y palabras clave

    // Procesar cada fila del CSV
    datosFiltrados.foreach { fila =>
      for {
        movieId <- Try(fila("id").trim.toInt).toOption // Obtener el ID de la película de forma segura
        jsonStr = fila.getOrElse("keywords", "").trim if jsonStr.nonEmpty && jsonStr != "\"\"" // Validar y limpiar el JSON
        jsonLimpio = limpiarJson(jsonStr).replaceAll("'", "\"") // Limpiar y reemplazar comillas simples por dobles
        jsonArray <- Try(Json.parse(jsonLimpio).as[JsArray]).toOption // Parsear el JSON a un array de objetos
      } jsonArray.value.foreach { obj =>
        // Validar y convertir cada objeto JSON en una instancia de Keyword
        obj.validate[Keyword] match {
          case JsSuccess(keyword, _) =>
            // Si la validación es exitosa, agregar la palabra clave a la lista
            keywordsList += keyword
            // Crear la relación entre la película y la palabra clave
            movieKeywordsList += ((movieId, keyword.id))
          case JsError(errors) =>
            // Si ocurre un error al validar el JSON, imprimir los errores
            println(s"Error en el JSON de keywords: $errors")
        }
      }
    }

    // Generar el archivo SQL con las inserciones
    try {
      // Inserciones para la tabla keywords (palabras clave)
      writer.println("-- Inserts para la tabla keywords")
      keywordsList.groupBy(_.id).foreach { case (idKeyword, keywordList) =>
        val keyword = keywordList.head // Obtener el primer elemento para este id (las palabras clave deben ser únicas por id)
        val nameEscaped = keyword.name.replace("'", "''") // Escapar las comillas simples en el nombre de la palabra clave
        // Insertar la palabra clave en la tabla keywords
        writer.println(s"INSERT INTO keywords (id_keywords, keywords_name) VALUES ($idKeyword, '$nameEscaped');")
      }

      // Inserciones para la tabla movieKeywords (películas-palabras clave)
      writer.println("\n-- Inserts para la tabla movieKeywords")
      movieKeywordsList.distinct.foreach { case (movieId, keywordId) =>
        // Insertar la relación entre películas y palabras clave
        writer.println(s"INSERT INTO movieKeywords (movie_id, id_keywords) VALUES ($movieId, $keywordId);")
      }

      println("Archivo SQL generado exitosamente: insertsKeywords.sql")
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
