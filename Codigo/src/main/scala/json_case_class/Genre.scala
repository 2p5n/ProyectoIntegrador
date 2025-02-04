package json_case_class

import play.api.libs.json.*

case class Genre(
                   id: Int,
                   name: String
                 )

// Implementar Reads para Play JSON
object Genre {
  implicit val genreReads: Reads[Genre] = Json.reads[Genre]
}
