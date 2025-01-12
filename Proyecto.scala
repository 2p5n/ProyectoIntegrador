import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}

object Proyecto {
  @main
  def main() : Unit = {
    implicit object CSVFormatter extends  DefaultCSVFormat{
      override val delimiter: Char = ';'
    }
    val path = "C:\\Users\\jdgue\\OneDrive\\Documentos\\Universidad Oct-Feb 2025\\ProyectoIntegrador\\pi_movies_small.csv"

    val reader = CSVReader.open(path)
    val dataMap: List[Map[String, String]] = reader.allWithHeaders()

    val listBudget: List[Long] = dataMap.
      flatMap(row => row.get("budget").
        flatMap(bdgt => scala.util.Try(bdgt.toLong).toOption))
    val sumCount: Tuple2[Double, Int] =
      listBudget
        .foldLeft((0.0, 0))((t2, currVal) => (t2._1 + currVal, t2._2 + 1))

    // Cálculo de la moda excluyendo 0
    // Cálculo de la moda
    val mode = listBudget
      .filter(_ != 0)
      .groupBy(x => x)
      .map(t => (t._1, t._2.length))
      .maxBy(_._2)
      ._1

    val desviacionEstandar = math.sqrt(listBudget
      .map(x => math.pow(x - sumCount._1 / sumCount._2, 2))
      .sum / listBudget.length)

    val avg = listBudget.filter(_>0).foldLeft(0.0, 0)((acc, currVal) => (acc._1 + currVal, acc._2 + 1))

    val listAdult: List[String] = dataMap.flatMap(row => row.get("adult"))
    val falso = listAdult.groupBy(identity).map(t => (t._1, t._2.length))

    println(falso)
    listAdult.distinct.foreach(println)
    println(s"min.: ${listBudget.min}")
    println(s"avg.: $avg")
    println(s"max.: ${listBudget.max}")
    println(s"desviacion = $desviacionEstandar")
    println(s"moda: $mode")
    reader.close()
  }
}


