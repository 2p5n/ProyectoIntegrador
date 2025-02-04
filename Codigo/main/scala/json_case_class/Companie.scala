package json_case_class

import play.api.libs.json.*

case class Companie(
                     id: Int,
                     name: String
                   )

object Companie {
  implicit val companieReads: Reads[Companie] = Json.reads[Companie]
}
