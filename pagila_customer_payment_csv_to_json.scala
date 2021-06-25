import com.github.tototoshi.csv._
import java.io._
import java.util.concurrent.atomic.AtomicInteger

implicit object MyFormat extends DefaultCSVFormat {
  override val delimiter = '|'
}

val fileName = "pagila_customer_payment.csv"
val indexName = "pagila_customer_payment"
val reader = CSVReader.open(new File(s"./$fileName")).toStreamWithHeaders

val prefix = fileName.substring(0, fileName.lastIndexOf('.'))
val output = new PrintStream(new FileOutputStream(s"./${prefix}.es.json"), true)

def getInstrHeader(i : Int) = s"""{ "index": { "_index": "$indexName", "_id": $i }}"""
val id = new AtomicInteger(1)

def mapPagilaCustomerPaymentToJson(pl: Map[String,String]) : String = {
	s"""{"customer_id" : ${pl("customer_id")},"store_id" : "${pl("store_id")}", "first_name" : "${pl("first_name")}", "last_name" : "${pl("last_name")}","email" : "${pl("email")}", "address_id" : ${pl("address_id")}, "activebool" : ${pl("activebool")},"create_date" : ${pl("create_date")}, "last_update" :${pl.getOrElse("last_update", "")},"active" : ${pl("active")}, "payment_id" : ${pl.getOrElse("payment_id", "")},"customer_id" : ${pl("customer_id")}, "staff_id" : ${pl("staff_id")}, "rental_id" : ${pl("rental_id")}, "amount" : ${pl("amount").trim}, "payment_date" : ${pl("payment_date")}}"""
}

reader.foreach((item: Map[String,String]) => {
	output.println(getInstrHeader(id.getAndIncrement))
	output.println(mapPagilaCustomerPaymentToJson(item))
})
output.flush
output.close
