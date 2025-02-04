package limpieza_inserts

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}  // Librería para leer archivos CSV
import json_case_class.Country  // Importa la clase case class para representar un país
import play.api.libs.json.*  // Librerías de Play Framework para trabajar con JSON
import utilidades.EscapeCaracteresEspeciales.limpiarJson  // Función personalizada para limpiar caracteres especiales en JSON

import java.io.{File, PrintWriter}  // Para manejar la escritura de archivos
import scala.collection.mutable.ListBuffer  // Para usar listas mutables durante el procesamiento
import scala.util.{Failure, Success, Try}  // Para manejar excepciones de manera funcional

object Production_countries extends App {

  // Rutas para los archivos de entrada y salida
  val rutaCsv = "data/pi_movies_complete.csv"  // Archivo CSV de entrada
  val rutaArchivoSql = "data/inserts_production_countries.sql"  // Archivo SQL de salida

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

    // Listas donde se almacenarán los países y las relaciones entre películas y países
    val countriesList = new ListBuffer[Country]()  // Lista para almacenar los países
    val movieCountriesList = new ListBuffer[(Int, String)]()  // Relación de IDs de películas y códigos de países

    // Recorre cada fila del CSV filtrado
    datosFiltrados.foreach { fila =>
      // Intenta extraer el ID de la película y procesar el campo JSON de los países de producción
      for {
        movieId <- Try(fila("id").trim.toInt).toOption  // Obtiene el ID de la película
        jsonStr = fila.getOrElse("production_countries", "").trim if jsonStr.nonEmpty && jsonStr != "\"\""  // Extrae y valida el JSON de países
        jsonLimpio = limpiarJson(jsonStr).replaceAll("'", "\"")  // Limpia el JSON y reemplaza comillas simples por dobles
        jsonArray <- Try(Json.parse(jsonLimpio).as[JsArray]).toOption  // Parsea el JSON limpio como un array de objetos
      } jsonArray.value.foreach {
        // Para cada objeto en el array de países, extrae el código ISO y el nombre del país
        case jsObj: JsObject =>
          // Extrae el código ISO del país y el nombre
          (jsObj \ "iso_3166_1").asOpt[String].foreach { iso =>
            val countryName = (jsObj \ "name").asOpt[String].getOrElse("null")  // Si no se encuentra el nombre, usa "null" como valor predeterminado
            val country = Country(iso, countryName)  // Crea una instancia de la clase Country
            countriesList += country  // Agrega el país a la lista de países
            movieCountriesList += ((movieId, iso))  // Agrega la relación película-país a la lista de relaciones
          }
        case _ => println(s"Formato inesperado en JSON de production_countries: $jsonLimpio")  // Maneja cualquier formato inesperado en el JSON
      }
    }

    try {
      // Escribe los inserts para la tabla 'countries' en el archivo SQL
      writer.println("-- Inserts para la tabla countries")
      countriesList.distinct.foreach { country =>
        val nameEscaped = country.name.replace("'", "''")  // Escapa las comillas simples en el nombre del país
        writer.println(s"INSERT INTO countries (iso_3166_1, countries_name) VALUES ('${country.iso_3166_1}', '$nameEscaped');")
      }

      // Escribe los inserts para la tabla 'movie_countries' en el archivo SQL
      writer.println("\n-- Inserts para la tabla movie_countries")
      movieCountriesList.distinct.foreach { case (movieId, iso) =>
        writer.println(s"INSERT INTO movie_countries (movie_id, iso_3166_1) VALUES ($movieId, '$iso');")
      }

      println("Archivo SQL generado exitosamente: inserts_production_countries.sql")  // Mensaje de éxito

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
