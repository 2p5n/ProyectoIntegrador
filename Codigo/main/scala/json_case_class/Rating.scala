package json_case_class

import play.api.libs.json.*

case class Rating(
                   userId: Int,
                   rating: Double,
                   timestamp: Long
                 )

// Implementar `Reads` para Play JSON
object Rating {
  implicit val ratingReads: Reads[Rating] = Json.reads[Rating]
}
