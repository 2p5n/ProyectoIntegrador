package limpieza_inserts

import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import play.api.libs.json.{JsObject, Json}
import utilidades.EscapeCaracteresEspeciales.limpiarJson

import java.io.{File, PrintWriter}
import scala.collection.mutable.{ListBuffer, Map}
import scala.util.{Failure, Success, Try}

object Crew extends App {
  // Definición de las rutas de los archivos CSV y SQL
  val rutaCsv = "data/pi_movies_complete.csv" // Ruta al archivo CSV de películas
  val rutaArchivoSql = "data/insertsCrew.sql" // Ruta al archivo SQL de salida

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
    val datosFiltrados = datos
      .filter { fila =>
        val columnasExcluyendoId = fila - "id" // Excluir la columna "id"
        // Filtrar filas con valores no vacíos y que no contengan "<unset>"
        columnasExcluyendoId.values.exists(_.trim.nonEmpty) &&
          !columnasExcluyendoId.values.forall(valor => valor.trim.toLowerCase == "<unset>")
      }
      .distinct // Eliminar filas duplicadas

    println(s"Total de filas después de filtrar: ${datosFiltrados.size}")

    // Listas para almacenar los datos que se insertarán en las tablas
    val personnelList = new ListBuffer[Map[String, Any]]() // Almacena información del personal
    val moviePersonnelList = new ListBuffer[(Int, Int, String)]() // Relación película-personal
    val jobList = new ListBuffer[String]() // Lista de trabajos
    val departmentList = new ListBuffer[String]() // Lista de departamentos
    val personnelJobList = new ListBuffer[(Int, Int)]() // Relación personal-trabajo
    val jobDepartmentMap = Map[String, String]() // Mapa de trabajos a departamentos

    // Procesar cada fila del CSV
    datosFiltrados.foreach { fila =>
      val movieIdStr = fila.getOrElse("id", "").trim
      val jsonStr = fila.getOrElse("crew", "").trim

      // Si el campo "crew" contiene datos, procesarlos
      if (jsonStr.nonEmpty && jsonStr != "\"\"") {
        val movieIdOpt = Try(movieIdStr.toInt).toOption // Obtener el ID de la película de forma segura

        movieIdOpt.foreach { movieId =>
          // Limpiar y parsear el JSON de "crew"
          val jsonLimpio = limpiarJson(jsonStr).replaceAll("'", "\"")

          // Intentar parsear el JSON como una lista de objetos JSON
          Try(Json.parse(jsonLimpio).as[List[JsObject]]) match {
            case Success(jsonArray) =>
              jsonArray.foreach { jsonObj =>
                // Extraer los datos de cada miembro del personal
                val creditId = (jsonObj \ "credit_id").asOpt[String].getOrElse("null")
                val department = (jsonObj \ "department").asOpt[String].filter(_.nonEmpty).getOrElse("null")
                val gender = (jsonObj \ "gender").asOpt[Int].getOrElse(-1)
                val idPersonnel = (jsonObj \ "id").asOpt[Int].getOrElse(-1)
                val job = (jsonObj \ "job").asOpt[String].filter(_.nonEmpty).getOrElse("null")
                val name = (jsonObj \ "name").asOpt[String].filter(_.nonEmpty).getOrElse("null")
                val profilePath = (jsonObj \ "profile_path").asOpt[String].getOrElse("null")

                // Almacenar la información del personal
                personnelList += Map(
                  "id_personnel" -> idPersonnel,
                  "name" -> name,
                  "gender" -> gender,
                  "profile_path" -> profilePath
                )

                // Almacenar la relación película-personal
                moviePersonnelList += ((movieId, idPersonnel, creditId))

                // Agregar el trabajo y departamento a las listas si no están presentes
                if (!jobList.contains(job)) jobList += job
                if (!departmentList.contains(department)) departmentList += department
                jobDepartmentMap(job) = department

                // Almacenar la relación entre personal y trabajo
                personnelJobList += ((idPersonnel, jobList.indexOf(job) + 1))
              }
            case Failure(exception) =>
              println(s"Error al parsear JSON de crew para Movie ID: $movieId: ${exception.getMessage}\nJSON: $jsonLimpio")
          }
        }
      }
    }

    // Generar el archivo SQL con las inserciones
    try {
      // Inserciones para la tabla personnel (personal)
      writer.println("-- Inserts para la tabla personnel")
      personnelList.distinct.foreach { person =>
        val nameEscaped = person("name").toString.replace("'", "''") // Escapar comillas simples
        val profilePathEscaped = person("profile_path").toString.replace("'", "''")
        val gender = person("gender")
        val idPersonnel = person("id_personnel")

        // Insertar la información del personal en la tabla
        writer.println(
          s"INSERT INTO personnel (id_personnel, personnel_name, gender, profile_path) VALUES ($idPersonnel, '$nameEscaped', $gender, '$profilePathEscaped');"
        )
      }

      // Inserciones para la tabla moviePersonnel (película-personal)
      writer.println("\n-- Inserts para la tabla moviePersonnel")
      moviePersonnelList.distinct.foreach { relation =>
        writer.println(s"INSERT INTO moviePersonnel (movie_id, id_personnel, credit_id) VALUES (${relation._1}, ${relation._2}, '${relation._3}');")
      }

      // Inserciones para la tabla department (departamentos)
      writer.println("\n-- Inserts para la tabla department")
      departmentList.distinct.zipWithIndex.foreach { case (department, index) =>
        val departmentEscaped = department.replace("'", "''")
        writer.println(s"INSERT INTO department (id_department, department_name) VALUES (${index + 1}, '$departmentEscaped');")
      }

      // Inserciones para la tabla job (trabajos)
      writer.println("\n-- Inserts para la tabla job")
      jobList.distinct.zipWithIndex.foreach { case (job, index) =>
        val jobEscaped = job.replace("'", "''")
        val idDepartment = departmentList.indexOf(jobDepartmentMap.getOrElse(job, "null")) + 1

        // Insertar el trabajo en la tabla job
        if (idDepartment > 0) {
          writer.println(s"INSERT INTO job (id_job, job_name, id_department) VALUES (${index + 1}, '$jobEscaped', $idDepartment);")
        } else {
          println(s"Advertencia: No se encontró id_department para el job '$job'")
        }
      }

      // Inserciones para la tabla personnelJob (personal-trabajo)
      writer.println("\n-- Inserts para la tabla personnelJob")
      personnelJobList.distinct.foreach { relation =>
        writer.println(s"INSERT INTO personnelJob (id_personnel, id_job) VALUES (${relation._1}, ${relation._2});")
      }

      println("Archivo SQL generado exitosamente: insertsCrew.sql")
    } catch {
      case e: Exception => println(s"Error al escribir el archivo SQL: ${e.getMessage}")
    } finally {
      writer.close() // Cerrar el archivo SQL después de escribir
    }

  } catch {
    case e: Exception => println(s"Error crítico: ${e.getMessage}") // Manejar errores generales
  } finally {
    reader.close() // Cerrar el archivo CSV después de leer
  }
}
