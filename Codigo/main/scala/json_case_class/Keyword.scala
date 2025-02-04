package json_case_class

import play.api.libs.json.{Json, Reads}

case class Keyword(
                   id: Int,
                   name: String
                   )
object Keyword {
  implicit val genreReads: Reads[Keyword] = Json.reads[Keyword]
}
