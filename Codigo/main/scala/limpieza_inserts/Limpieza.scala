import com.github.tototoshi.csv._  // Librería para trabajar con archivos CSV
import java.io.File  // Para trabajar con archivos locales
import play.api.libs.json._  // Para trabajar con JSON (aunque no se usa explícitamente en este código)
import play.api.libs.functional.syntax._  // Para trabajar con sintaxis funcional de Play Framework
import movie_case_class._  // Importa las clases del caso de películas
import scala.util.{Try, Success, Failure}  // Para manejar excepciones de manera funcional
import sql_codigo.codigo  // Funciones relacionadas con operaciones SQL
import cats.effect.unsafe.implicits.global  // Requerido para la ejecución de efectos de IO
import cats.effect.{IO, Resource}  // Para trabajar con efectos IO en programación funcional
import doobie.hikari.HikariTransactor  // Para gestionar la conexión con la base de datos
import doobie.implicits._  // Para las operaciones SQL con Doobie

object Limpieza extends App {

  val rutaCsv = "src/main/resources/data/pi_movies_complete.csv"  // Ruta del archivo CSV

  // Se define el formato del archivo CSV, en este caso el delimitador es punto y coma (;)
  implicit val csvFormat: DefaultCSVFormat = new DefaultCSVFormat {
    override val delimiter = ';' // Delimitador usado en el archivo
  }

  val reader = CSVReader.open(new File(rutaCsv))  // Abre el archivo CSV para leerlo

  try {
    val datos = reader.allWithHeaders()  // Lee todas las filas del CSV, manteniendo los encabezados

    // Filtra filas vacías o con todas las columnas (excepto id) como <unset>
    val datosFiltrados = datos.filter { fila =>
      val columnasExcluyendoId = fila - "id"  // Excluye la columna "id" para comprobar
      columnasExcluyendoId.values.exists(_.trim.nonEmpty) &&  // Asegura que al menos una columna tiene datos
        !columnasExcluyendoId.values.forall(valor => valor.trim.toLowerCase == "<unset>")  // Excluye filas con solo valores "<unset>"
    }

    println(s"Total de filas después de eliminar vacías y <unset>: ${datosFiltrados.size}")
    println(s"Total de filas leídas: ${datos.size}")
    println(s"Total de filas después de eliminar vacías y <unset>: ${datosFiltrados.size}")

    // Validar cada fila y categorizar entre válidas e inválidas
    val resultados = datosFiltrados.map { fila =>
      Try {
        // Validar id: Debe ser un número
        val id = fila.getOrElse("id", "").trim
        require(id.matches("^\\d+$"), s"ID inválido: $id")

        // Validar original_title: No debe contener JSON ni ser nulo
        val originalTitle = fila.getOrElse("original_title", "").trim
        require(!originalTitle.startsWith("{") && !originalTitle.startsWith("["), s"JSON en original_title: $originalTitle")
        require(originalTitle.nonEmpty && !originalTitle.equalsIgnoreCase("null"), "Título original inválido")

        fila  // Si todas las validaciones pasan, retorna la fila
      } match {
        case Success(validFila) => Right(validFila)
        case Failure(exception: Exception) => Left((fila, exception.getMessage))
      }
    }

    // Separar filas válidas e inválidas
    val datosValidos = resultados.collect { case Right(fila: Map[String, String]) => fila }
    val datosInvalidos = resultados.collect { case Left((fila, error)) => (fila, error) }

    println(s"Total de filas válidas: ${datosValidos.size}")
    println(s"Total de filas inválidas: ${datosInvalidos.size}")

    if (datosInvalidos.nonEmpty) {
      println("\n=== Errores encontrados ===")
      datosInvalidos.take(10).foreach { case (_, error) =>
        println(s"- $error")
      }
    }

    // Limpieza de los datos
    val datosLimpios = datos.map { fila =>
      // Limpieza de datos en cada columna
      val adultLimpio = fila.getOrElse("adult", "").trim.toLowerCase match {
        case "true" | "1" | "yes" => "1"
        case "false" | "0" | "no" | "" => "0"
        case _ => "0"
      }

      val belongsToCollectionLimpio = fila.getOrElse("belongs_to_collection", "NULL").trim
      val budgetLimpio = fila.getOrElse("budget", "0").replaceAll("[^\\d]", "").trim.toIntOption.getOrElse(0)
      val genresLimpio = fila.getOrElse("genres", "NULL").trim
      val homepageLimpio = fila.getOrElse("homepage", "").trim match {
        case url if url.matches("^(https?|ftp)://[\\w.-]+(?:\\.[\\w.-]+)+[/\\w\\._~:?#[\\\\]@!$&'()+,;=-]$") => url
        case _ => "NULL"
      }

      val idLimpio = fila.getOrElse("id", "0").replaceAll("[^\\d]", "").trim.toIntOption.getOrElse(0)
      val imdbIdLimpio = fila.getOrElse("imdb_id", "").trim match {
        case imdb if imdb.matches("^tt\\d{7,8}$") => imdb
        case _ => "NULL"
      }

      val originalLanguageLimpio = fila.getOrElse("original_language", "").trim.toLowerCase match {
        case lang if lang.matches("^[a-z]{2,3}$") => lang
        case _ => "NULL"
      }

      val originalTitleLimpio = fila.getOrElse("original_title", "NULL").trim.capitalize
      val overviewLimpio = fila.getOrElse("overview", "NULL").trim.capitalize

      val popularityLimpio = fila.getOrElse("popularity", "").trim.toDoubleOption.getOrElse(0.0)

      val posterPathLimpio = fila.getOrElse("poster_path", "").trim match {
        case path if path.matches("^/[a-zA-Z0-9._-]+\\.(jpg|png|jpeg|gif)$") => path
        case _ => "NULL"
      }

      val productionCompaniesLimpio = fila.getOrElse("production_companies", "NULL").trim
      val productionCountriesLimpio = fila.getOrElse("production_countries", "NULL").trim
      val releaseDateLimpio = fila.getOrElse("release_date", "").trim match {
        case date if date.matches("^\\d{4}-\\d{2}-\\d{2}$") => date
        case _ => "NULL"
      }

      val revenueLimpio = fila.getOrElse("revenue", "0").replaceAll("[^\\d]", "").trim.toIntOption.getOrElse(0)
      val runtimeLimpio = fila.getOrElse("runtime", "").trim.toIntOption

      val spokenLanguagesLimpio = fila.getOrElse("spoken_languages", "NULL").trim
      val statusLimpio = fila.getOrElse("status", "NULL").trim.capitalize
      val taglineLimpio = fila.getOrElse("tagline", "NULL").trim.capitalize
      val titleLimpio = fila.getOrElse("title", "NULL").trim.capitalize

      val videoLimpio = fila.getOrElse("video", "").trim.toLowerCase match {
        case v if Seq("true", "1", "yes").contains(v) => "1"
        case _ => "0"
      }

      val voteAverageLimpio = fila.getOrElse("vote_average", "").trim.toDoubleOption.getOrElse(0.0)
      val voteCountLimpio = fila.getOrElse("vote_count", "").trim.toIntOption.getOrElse(0)

      val keywordsLimpio = fila.getOrElse("keywords", "NULL").trim
      val castLimpio = fila.getOrElse("cast", "NULL").trim
      val crewLimpio = fila.getOrElse("crew", "NULL").trim
      val ratingsLimpio = fila.getOrElse("ratings", "NULL").trim

      // Instanciación del objeto con los valores limpios
      movie_cc(
        adult = adultLimpio,
        belongs_to_collection = belongsToCollectionLimpio,
        budget = budgetLimpio,
        genres = genresLimpio,
        homepage = homepageLimpio,
        id = idLimpio,
        imdb_id = imdbIdLimpio,
        original_language = originalLanguageLimpio,
        original_title = originalTitleLimpio,
        overview = overviewLimpio,
        popularity = popularityLimpio,
        poster_path = posterPathLimpio,
        production_companies = productionCompaniesLimpio,
        production_countries = productionCountriesLimpio,
        release_date = releaseDateLimpio,
        revenue = revenueLimpio,
        runtime = runtimeLimpio,
        spoken_languages = spokenLanguagesLimpio,
        status = statusLimpio,
        tagline = taglineLimpio,
        title = titleLimpio,
        video = videoLimpio,
        vote_average = voteAverageLimpio,
        vote_count = voteCountLimpio,
        keywords = keywordsLimpio,
        cast = castLimpio,
        crew = crewLimpio,
        ratings = ratingsLimpio
      )
    }

    // Elimina las películas duplicadas por ID
    val pelilimpias = datosLimpios.distinctBy(_.id)

    // Transforma los objetos movie_cc a movie_table, para insertar en la base de datos
    val movie_table_list: List[movie_table] = pelilimpias.map { x =>
      movie_table(
        adult = x.adult,
        budget = x.budget,
        homepage = x.homepage,
        id = x.id,
        imdb_id = x.imdb_id,
        original_language = x.original_language,
        original_title = x.original_title,
        overview = x.overview,
        popularity = x.popularity,
        poster_path = x.poster_path,
        release_date = x.release_date,
        revenue = x.revenue.toDouble, // Asegurar que coincide con el tipo Double
        runtime = x.runtime.getOrElse(0), // Si runtime es Option[Int], extrae el valor o usa 0
        status = x.status,
        tagline = x.tagline,
        title = x.title,
        video = x.video,
        vote_average = x.vote_average,
        vote_count = x.vote_count.toDouble // Asegurar que coincide con el tipo Double
      )
    }.toList

    // Función que inserta los datos en la base de datos
    def insert_movie(): IO[Unit] = {
      codigo.insertAll(movie_table_list)
        .flatMap(result => IO.println(s"FILAS DE MOVIE: ${result.size}"))
    }

    // Ejecuta la inserción
    insert_movie().unsafeRunSync()

  } catch {
    case e: Exception =>
      println(s"Se ha producido un error durante el proceso de limpieza: ${e.getMessage}")
      e.printStackTrace()
  } finally {
    reader.close()  // Cierra el lector del archivo CSV al finalizar
  }
}
