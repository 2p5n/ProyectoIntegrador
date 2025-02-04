package json_case_class

import play.api.libs.json.{Json, Reads}

case class spoken_languages(
                              iso_639_1: String,
                              name: String
                              )
object spoken_languages {
  implicit val genreReads: Reads[spoken_languages] = Json.reads[spoken_languages]
}