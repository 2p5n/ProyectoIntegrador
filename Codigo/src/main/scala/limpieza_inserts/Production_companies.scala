package limpieza_inserts

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}  // Librería para leer archivos CSV
import json_case_class.Companie  // Importa la clase case class para representar una compañía
import play.api.libs.json.*  // Librerías de Play Framework para trabajar con JSON
import utilidades.EscapeCaracteresEspeciales.limpiarJson  // Función personalizada para limpiar caracteres especiales en JSON

import java.io.{File, PrintWriter}  // Para manejar la escritura de archivos
import scala.collection.mutable.ListBuffer  // Para usar listas mutables durante el procesamiento
import scala.util.{Failure, Success, Try}  // Para manejar excepciones de manera funcional

object Production_companies extends App {

  // Rutas para los archivos de entrada y salida
  val rutaCsv = "data/pi_movies_complete.csv"  // Archivo CSV de entrada
  val rutaArchivoSql = "data/inserts_production_companies.sql"  // Archivo SQL de salida

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

    // Listas donde se almacenarán las compañías y las relaciones entre películas y compañías
    val companiesList = new ListBuffer[Companie]()
    val movieCompaniesList = new ListBuffer[(Int, Int)]()  // Relación de IDs de películas y compañías

    // Recorre cada fila del CSV filtrado
    datosFiltrados.foreach { fila =>
      // Intenta extraer el ID de la película y procesar el campo JSON de las compañías de producción
      for {
        movieId <- Try(fila("id").trim.toInt).toOption  // Obtiene el ID de la película
        jsonStr = fila.getOrElse("production_companies", "").trim if jsonStr.nonEmpty && jsonStr != "\"\""  // Extrae y valida el JSON de compañías
        jsonLimpio = limpiarJson(jsonStr).replaceAll("'", "\"")  // Limpia el JSON y reemplaza comillas simples por dobles
        jsonArray <- Try(Json.parse(jsonLimpio).as[JsArray]).toOption  // Parsea el JSON limpio como un array de objetos
      } jsonArray.value.foreach { obj =>
        // Valida y mapea cada objeto JSON a una instancia de la clase Companie
        obj.validate[Companie] match {
          case JsSuccess(company, _) =>
            companiesList += company  // Agrega la compañía a la lista de compañías
            movieCompaniesList += ((movieId, company.id))  // Agrega la relación película-compañía a la lista de relaciones
          case JsError(errors) =>
            println(s"Error en el JSON de production_companies: $errors")  // Imprime los errores de validación si los hay
        }
      }
    }

    try {
      // Escribe los inserts para la tabla 'company' en el archivo SQL
      writer.println("-- Inserts para la tabla companies")
      companiesList.groupBy(_.id).foreach { case (idCompany, companyList) =>
        val company = companyList.head  // Obtiene la primera compañía (en caso de duplicados)
        val nameEscaped = company.name.replace("'", "''")  // Escapa las comillas simples en el nombre de la compañía
        writer.println(s"INSERT INTO company (id_company, company_name) VALUES ($idCompany, '$nameEscaped');")
      }

      // Escribe los inserts para la tabla 'movie_companies' en el archivo SQL
      writer.println("\n-- Inserts para la tabla movie_companies")
      movieCompaniesList.distinct.foreach { case (movieId, companyId) =>
        writer.println(s"INSERT INTO moviecompanies (movie_id, id_company) VALUES ($movieId, $companyId);")
      }

      println("Archivo SQL generado exitosamente: inserts_production_companies.sql")  // Mensaje de éxito

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
