package json_cc_modelo_logico

import play.api.libs.json._

case class MovieGenre(
                       movieId: Int,
                       genreId: Int
                     )

object MovieGenre {
  implicit val movieGenreReads: Reads[MovieGenre] = Json.reads[MovieGenre]
}
