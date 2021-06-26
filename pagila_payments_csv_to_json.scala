import com.github.tototoshi.csv._
import java.io._
import java.util.concurrent.atomic.AtomicInteger

implicit object MyFormat extends DefaultCSVFormat {
  override val delimiter = '|'
}

val fileName = "pagila_payments.csv"
val indexName = "pagila_payments"
val reader = CSVReader.open(new File(s"./$fileName")).toStreamWithHeaders

val prefix = fileName.substring(0, fileName.lastIndexOf('.'))
val output = new PrintStream(new FileOutputStream(s"./${prefix}.es.json"), true)

def getInstrHeader(i : Int) = s"""{ "index": { "_index": "$indexName", "_id": $i }}"""
val id = new AtomicInteger(1)

def mapPagilaPaymentsToJson(pl: Map[String,String]) : String = {
	s"""{"payment_id" : ${pl("payment_id")},"customer_id" : "${pl("customer_id")}", "staff_id" : "${pl("staff_id")}", "rental_id" : "${pl("rental_id")}","amount" : "${pl("amount")}","payment_date" : ${pl("payment_date")},"staff_first_name" : ${pl("staff_first_name")}, "staff_last_name" :${pl.getOrElse("staff_last_name", "")},"address_id" : ${pl("address_id")}, "email" : ${pl.getOrElse("email", "")},"store_id" : ${pl("store_id")}, "active" : ${pl("active")}, "username" : ${pl("username")}, "rental_date" : ${pl("rental_date").trim}, "customer_id" : ${pl("customer_id")}, "return_date" : ${pl("return_date")}, "last_update" : ${pl("last_update")}, "store_id" : ${pl("store_id").trim}, "last_update" : ${pl("last_update")}, "film_id" :${pl.getOrElse("film_id", "")},"title" : ${pl("title")}, "description" : ${pl.getOrElse("description", "")},"release_year" : ${pl("release_year")}, "language_id" : ${pl("language_id")}, "original_language_id" : ${pl("original_language_id")}, "rental_duration" : ${pl("rental_duration").trim}, "rental_rate" : ${pl("rental_rate")}, "length" : ${pl("length")}, "replacement_cost" : ${pl("replacement_cost")}, "rating" : ${pl("rating").trim}, "last_update" : ${pl("last_update")}, "special_features" : ${pl("special_features")}, "category" : ${pl("category").trim}, "language" : ${pl("language")}}"""
}

reader.foreach((item: Map[String,String]) => {
	output.println(getInstrHeader(id.getAndIncrement))
	output.println(mapPagilaPaymentsToJson(item))
})
output.flush
output.close
