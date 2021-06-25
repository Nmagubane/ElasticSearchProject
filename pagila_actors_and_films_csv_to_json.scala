import com.github.tototoshi.csv._
import java.io._
import java.util.concurrent.atomic.AtomicInteger

implicit object MyFormat extends DefaultCSVFormat {
  override val delimiter = '|'
}

val fileName = "pagila_actors_and_films.csv"
val indexName = "pagila_actors_and_films"
val reader = CSVReader.open(new File(s"./$fileName")).toStreamWithHeaders

val prefix = fileName.substring(0, fileName.lastIndexOf('.'))
val output = new PrintStream(new FileOutputStream(s"./${prefix}.es.json"), true)

def getInstrHeader(i : Int) = s"""{ "index": { "_index": "$indexName", "_id": $i }}"""
val id = new AtomicInteger(1)

def mapPagilaActorFilmsToJson(pl: Map[String,String]) : String = {
	s"""{"film_id" : ${pl("film_id")},"actor_first_name" : "${pl("actor_first_name")}", "actor_last_name" : "${pl("actor_last_name")}", "title" : "${pl("title")}","description" : "${pl("description")}", "release_year" : ${pl("release_year")}, "rental_duration" : ${pl("rental_duration")},"rental_rate" : ${pl("rental_rate")}, "length" :${pl.getOrElse("length", "")},"replacement_cost" : ${pl("replacement_cost")}, "rating" : "${pl.getOrElse("rating", "")}","special_features" : "${pl("special_features")}", "category" : "${pl("category")}", "language" : "${pl("language").trim}"}"""
}

reader.foreach((item: Map[String,String]) => {
	output.println(getInstrHeader(id.getAndIncrement))
	output.println(mapPagilaActorFilmsToJson(item))
})
output.flush
output.close
