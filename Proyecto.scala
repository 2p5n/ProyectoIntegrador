import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}

object Proyecto {
  def main(args: Array[String]): Unit = {
    implicit object CSVFormatter extends DefaultCSVFormat {
      override val delimiter: Char = ';'
    }
    val path = "C:\\Users\\jdgue\\OneDrive\\Documentos\\Universidad Oct-Feb 2025\\ProyectoIntegrador\\pi_movies_small.csv"
    val reader = CSVReader.open(path)
    val dataMap: List[Map[String, String]] = reader.allWithHeaders()

    def EDA(columm: String): Unit = {
      val list: List[Long] = dataMap.
        flatMap(row => row.get(columm).
          flatMap(bdgt => scala.util.Try(bdgt.toLong).toOption))

      if (list.nonEmpty) {
        val sumCount: (Double, Int) =
          list.foldLeft((0.0, 0))((t2, currVal) => (t2._1 + currVal, t2._2 + 1))

        val mode = list
          .filter(_ != 0)
          .groupBy(x => x)
          .map(t => (t._1, t._2.length))
          .maxByOption(_._2)
          .map(_._1)
          .getOrElse(0L)

        val desviacionEstandar = math.sqrt(
          list.map(x => math.pow(x - sumCount._1 / sumCount._2, 2)).sum / list.length
        )

        val avg = sumCount._1 / sumCount._2

        println(s"min.: ${list.min}")
        println(s"avg.: $avg")
        println(s"max.: ${list.max}")
        println(s"desviacion = $desviacionEstandar")
        println(s"moda: $mode")
      } else {
        println(s"No hay datos válidos en la columna $columm.")
      }



  }
    val listAdult: List[String] = dataMap.flatMap(row => row.get("adult"))
    val falso = listAdult.groupBy(identity).map(t => (t._1, t._2.length))
    println("Columna Adult: " + falso)
    val listLanguage: List[String] = dataMap.flatMap(row => row.get("original_language"))
    val language = listLanguage.groupBy(identity).map(t => (t._1, t._2.length))
    println("Columna Language: " + language)
    val listStatus: List[String] = dataMap.flatMap(row => row.get("status"))
    val status = listStatus.groupBy(identity).map(t => (t._1, t._2.length))
    println("Columna Status: " + status)
    val listVideo: List[String] = dataMap.flatMap(row => row.get("video"))
    val video = listVideo.groupBy(identity).map(t => (t._1, t._2.length))
    println("Columna Video: " + video)
    println("")

    println("Tabla Budget")
    EDA("budget")
    println("Tabla Revenue")
    EDA("revenue")
    println("Tabla Runtime")
    EDA("runtime")
    println("Tabla Vote_average")
    EDA("vote_average")
    println("Tabla Vote_count")
    EDA("vote_count")
  }
}
