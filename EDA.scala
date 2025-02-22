import movie_case_class.movie_cc
import json_case_class.{belongs_to_collection_cc, genres_cc, production_companies_cc, production_countries_cc,
  spoken_languages_cc, keywords_cc, cast_cc, crew_cc, ratings_cc}
import Limpieza._

object EDA extends App {
  val datosLimpios = Limpieza.datos
  eda_numerico(List("budget", "popularity", "revenue", "runtime", "vote_average", "vote_count"), datosLimpios)
  println("ANÁLISIS EXPLORATIVO DE FRECUENCIAS")
  eda_frecuencialBool(List("adult", "video"), datosLimpios)
  eda_frecuencial(List("original_language"), datosLimpios)
  eda_frecuencialStatus(List("status"), datosLimpios)
  def eda_numerico(columnas: List[String], datos: List[Map[String, String]]): Unit = {
    columnas.foreach{columna =>
      val lista: List[Double] = datos
        .flatMap(row => row.get(columna))
        .flatMap(values => scala.util.Try(values.toDouble).toOption)
      
      val sumCount: Tuple2[Double, Int] =
        lista
        .foldLeft((0.0, 0))((t2, currVal) => (t2._1 + currVal, t2._2 + 1))

      val mode: Option[Double] = lista
        .filter(_ != 0)
        .groupBy(identity)
        .map(t => (t._1, t._2.length))
        .toList
        .sortBy(-_._2) // Ordenar por frecuencia en orden descendente
        .headOption // Obtener el primero si existe
        .map(_._1) // Tomar el valor de la moda

      val desviacionEstandar = math.sqrt(lista
        .map(x => math.pow(x - sumCount._1 / sumCount._2, 2))
        .sum / lista.length)

      println("ANÁLISIS EXPLORATORIO DE DATOS DE: " + columna)
      println(s"min.: ${lista.min}")
      println(s"avg.: ${sumCount._1 / sumCount._2}")
      println(s"max.: ${lista.max}")
      println(s"desviacion = $desviacionEstandar")
      println(s"moda: $mode")
      println("=======================================")
    }
  }
  def eda_frecuencial(columnas: List[String], datos: List[Map[String, String]]): Unit = {
    columnas.foreach { columna =>
      // Eliminamos espacios y valores vacíos
      val valores = datos.flatMap(_.get(columna).map(_.trim)).filter(_.nonEmpty)

      val frecuencias = valores.groupBy(identity).view.mapValues(_.size).toMap

      // Ordenamos por frecuencia descendente
      val frecuenciasOrdenadas = frecuencias.toSeq.sortBy(-_._2)
      println("===========================================")
      println(s"\nFrecuencia en columna '$columna':")
      frecuenciasOrdenadas.foreach { case (valor, frecuencia) =>
        println(f"$valor%-30s -> $frecuencia")
      }
    }
  }
  def eda_frecuencialBool(columnas: List[String], datos: List[Map[String, String]]): Unit = {
    columnas.foreach { columna =>
      // Eliminamos espacios y valores vacíos
      val valores = datos.flatMap(_.get(columna).map(_.trim)).filter(_.nonEmpty)

      // Filtramos valores booleanos exclusivamente (true / false)
      val valoresFiltrados = valores.filter(v => v.equalsIgnoreCase("true") || v.equalsIgnoreCase("false"))

      // Calculamos frecuencias
      val frecuencias = valoresFiltrados.groupBy(identity).view.mapValues(_.size).toMap

      // Ordenamos por frecuencia descendente
      val frecuenciasOrdenadas = frecuencias.toSeq.sortBy(-_._2)

      println("===========================================")
      println(s"\nFrecuencia en columna '$columna':")
      frecuenciasOrdenadas.foreach { case (valor, frecuencia) =>
        println(f"$valor%-30s -> $frecuencia")
      }
    }
  }

  def eda_frecuencialStatus(columnas: List[String], datos: List[Map[String, String]]): Unit = {
    columnas.foreach { columna =>
      // Eliminamos espacios y valores vacíos
      val valores = datos.flatMap(_.get(columna).map(_.trim)).filter(_.nonEmpty)

      // Filtramos valores booleanos exclusivamente (true / false)
      val valoresFiltrados = valores.filter(v => v.equalsIgnoreCase("Released") || v.equalsIgnoreCase("Rumored") || v.equalsIgnoreCase("Post Pro") || v.equalsIgnoreCase("In Produ"))

      // Calculamos frecuencias
      val frecuencias = valoresFiltrados.groupBy(identity).view.mapValues(_.size).toMap

      // Ordenamos por frecuencia descendente
      val frecuenciasOrdenadas = frecuencias.toSeq.sortBy(-_._2)

      println("===========================================")
      println(s"\nFrecuencia en columna '$columna':")
      frecuenciasOrdenadas.foreach { case (valor, frecuencia) =>
        println(f"$valor%-30s -> $frecuencia")
      }
    }
  }
}