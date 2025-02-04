package movie_case_class

case class movie_cc(
                     adult: String,
                     belongs_to_collection: String,     //JSON
                     budget: Int,
                     genres: String,                    //JSON
                     homepage: String,
                     id: Int,
                     imdb_id: String,
                     original_language: String,
                     original_title: String,
                     overview: String,
                     popularity: Double,
                     poster_path: String,
                     production_companies: String,      //JSON
                     production_countries: String,      //JSON
                     release_date: String,
                     revenue: Int,
                     runtime: Option[Int],
                     spoken_languages: String,          //JSON
                     status: String,
                     tagline: String,
                     title: String,
                     video: String,
                     vote_average: Double,
                     vote_count: Int,
                     keywords: String,                  //JSON
                     cast: String,                      //JSON
                     crew: String,                      //JSON
                     ratings: String                    //JSON
                   )

