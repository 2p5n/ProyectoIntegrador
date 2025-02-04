package json_case_class

import play.api.libs.json.{Json, Reads}

case class Country(
                                  iso_3166_1: String,
                                  name: String
                                  )
object Country {
  implicit val countryReads: Reads[Country] = Json.reads[Country]
}