package limpieza_inserts

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}  // Librería para leer archivos CSV
import play.api.libs.json.{JsObject, Json}  // Librerías de Play Framework para trabajar con JSON
import utilidades.EscapeCaracteresEspeciales.limpiarJson  // Función personalizada para limpiar caracteres especiales en JSON

import java.io.{File, PrintWriter}  // Para manejar la escritura de archivos
import scala.util.{Failure, Success, Try}  // Para manejar excepciones de manera funcional

object Spoken_languages extends App {

  // Rutas para los archivos de entrada y salida
  val rutaCsv = "data/pi_movies_complete.csv"  // Archivo CSV de entrada
  val rutaArchivoSql = "data/inserts_spoken_languages.sql"  // Archivo SQL de salida

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

    // Listas donde se almacenarán los idiomas hablados y las relaciones entre películas e idiomas
    val spokenLanguagesList = scala.collection.mutable.Set[(String, String)]()  // Conjunto para almacenar los idiomas únicos (isoCode, name)
    val movieSpokenLanguagesList = scala.collection.mutable.ListBuffer[(Int, String)]()  // Relación entre película e idioma hablado

    // Recorre cada fila del CSV filtrado
    datosFiltrados.foreach { fila =>
      // Intenta extraer el ID de la película y procesar el campo JSON de idiomas hablados
      for {
        movieId <- Try(fila("id").trim.toInt).toOption  // Obtiene el ID de la película
        jsonStr = fila.getOrElse("spoken_languages", "").trim if jsonStr.nonEmpty && jsonStr != "\"\""  // Extrae y valida el JSON de idiomas hablados
        jsonLimpio = limpiarJson(jsonStr).replaceAll("'", "\"")  // Limpia el JSON y reemplaza comillas simples por dobles
        jsonArray <- Try(Json.parse(jsonLimpio).as[List[JsObject]]).toOption  // Parsea el JSON limpio como una lista de objetos JsObject
      } jsonArray.foreach { langObj =>
        // Extrae y valida los valores de idioma (isoCode y name)
        val isoCode = (langObj \ "iso_639_1").asOpt[String].filter(_.nonEmpty).getOrElse("null")  // Extrae el código ISO del idioma
        val name = (langObj \ "name").asOpt[String].filter(_.nonEmpty).getOrElse("null")  // Extrae el nombre del idioma
        spokenLanguagesList.add((isoCode, name))  // Añade el par (isoCode, name) a la lista de idiomas
        movieSpokenLanguagesList.append((movieId, isoCode))  // Añade la relación entre la película y el idioma a la lista
      }
    }

    try {
      // Escribe los inserts para la tabla 'languages' (idiomas) en el archivo SQL
      writer.println("-- Inserts para la tabla spoken_languages")
      spokenLanguagesList.foreach { case (isoCode, name) =>
        val nameEscaped = name.replace("'", "''")  // Escapa las comillas simples en el nombre del idioma
        writer.println(s"INSERT INTO languages (iso_639_1, languages_name) VALUES ('$isoCode', '$nameEscaped');")
        // Inserta cada idioma con su código ISO en la tabla 'languages'
      }

      // Escribe los inserts para la tabla 'movielanguages' (relación entre películas e idiomas hablados) en el archivo SQL
      writer.println("\n-- Inserts para la tabla movieSpokenLanguages")
      movieSpokenLanguagesList.distinct.foreach { case (movieId, isoCode) =>
        writer.println(s"INSERT INTO movielanguages (movie_id, iso_639_1) VALUES ($movieId, '$isoCode');")
        // Inserta la relación entre la película y el idioma en la tabla 'movielanguages'
      }

      println("Archivo SQL generado exitosamente: inserts_spoken_languages.sql")  // Mensaje de éxito

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
