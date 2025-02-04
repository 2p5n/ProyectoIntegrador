package limpieza_inserts

// Se importan las bibliotecas necesarias para manejar archivos CSV, JSON y para escritura de archivos.
import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import json_case_class.BelongsToCollection
import play.api.libs.json.*
import utilidades.EscapeCaracteresEspeciales.limpiarJsonCrew

import java.io.{File, PrintWriter}
import scala.util.{Failure, Success, Try}

// El objeto Belongs_to_collection extiende la aplicación principal.
object Belongs_to_collection extends App {
  // Definición de las rutas de los archivos de entrada y salida.
  val rutaCsv = "data/pi_movies_complete.csv"  // Ruta del archivo CSV de entrada.
  val rutaArchivoSql = "data/inserts_belongs_to_collection.sql"  // Ruta del archivo SQL de salida.

  // Definición de un formato personalizado para el CSV donde se utiliza el punto y coma (;) como delimitador.
  implicit val csvFormat: DefaultCSVFormat = new DefaultCSVFormat {
    override val delimiter = ';'  // El delimitador entre los campos en el CSV es el punto y coma.
  }

  // Se abre el archivo CSV para leer y el archivo SQL para escribir.
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

    // Función para parsear cadenas JSON a un objeto de tipo T utilizando un lector implícito.
    def parsearJson[T](jsonStr: String)(implicit reads: Reads[T]): Option[T] = {
      Try(Json.parse(jsonStr).as[T]).toOption
    }

    // Se mapea cada fila filtrada para extraer el ID de la película y su objeto JSON asociado con la colección.
    val belongsToCollectionData = datosFiltrados.flatMap { fila =>
      for {
        movieId <- Try(fila("id").trim.toInt).toOption  // Se obtiene el ID de la película.
        jsonStr = fila.getOrElse("belongs_to_collection", "").trim if jsonStr.nonEmpty && jsonStr != "\"\""  // Se obtiene el JSON de la colección.
        // Se limpia el JSON eliminando caracteres especiales.
        jsonLimpio = limpiarJsonCrew(jsonStr).replaceAll("'", "\"")
        jsonObj <- Try(Json.parse(jsonLimpio).validate[JsObject]).toOption.collect { case JsSuccess(obj, _) => obj }  // Se convierte a objeto JSON.
        id <- (jsonObj \ "id").asOpt[Int]  // Se obtiene el ID de la colección.
        name = (jsonObj \ "name").asOpt[String].filter(_.nonEmpty).getOrElse("null")  // Se obtiene el nombre de la colección, o "null" si no está presente.
        posterPath = (jsonObj \ "poster_path").asOpt[String].filter(_.nonEmpty).getOrElse("null")  // Se obtiene la ruta del póster.
        backdropPath = (jsonObj \ "backdrop_path").asOpt[String].filter(_.nonEmpty).getOrElse("null")  // Se obtiene la ruta del fondo.
        collection = BelongsToCollection(id, name, posterPath, backdropPath)  // Se crea un objeto de tipo BelongsToCollection.
      } yield (movieId, collection)  // Se retorna una tupla con el ID de la película y la colección.
    }.distinct

    // Se eliminan entradas duplicadas basadas en el ID de la película y la colección.
    val distinctData = belongsToCollectionData.distinctBy(_._1).distinctBy(identity)

    // Se escribe cada inserción de SQL en el archivo de salida.
    distinctData.foreach { case (_, collection) =>
      writer.println(s"INSERT INTO belongs_to_collection (collection_id, collection_name, collection_poster_path, collection_backdrop_path) VALUES (${collection.id}, '${collection.name}', '${collection.posterPath}', '${collection.backdropPath}');")
    }

    // Se imprime el número total de registros insertados.
    println(s"Total de registros insertados: ${belongsToCollectionData.size}")
  } catch {
    // Si ocurre un error, se captura y se imprime el mensaje del error.
    case e: Exception => println(s"Error crítico: ${e.getMessage}")
  } finally {
    // Se cierran los archivos de lectura y escritura al finalizar el procesamiento.
    reader.close()
    writer.close()
  }
}
