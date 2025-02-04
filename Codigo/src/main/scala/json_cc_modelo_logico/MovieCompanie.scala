package json_cc_modelo_logico

import play.api.libs.json._

case class MovieCompanie(
                          movieId: Int,
                          companyId: Int
                        )

object MovieCompanie {
  implicit val movieCompanieReads: Reads[MovieCompanie] = Json.reads[MovieCompanie]
}
