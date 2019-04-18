package stream

import akka.stream.Materializer
import akka.stream.alpakka.hbase.HTableSettings
import akka.stream.alpakka.hbase.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

class HBase(implicit materializer: Materializer) {
  val config = HBaseConfiguration.create()
  private val settings = HTableSettings(config, TableName.valueOf("book"), List("e", "r"),
    (b: Book) => {
      val put = new Put(Bytes.toBytes(b.no))
      put.addColumn(Bytes.toBytes("e"), Bytes.toBytes("title"), Bytes.toBytes(b.title))
      List(put)
    })

  def close() = {

  }

  def start(table: String) = {

  }

  def write(table: String, data: List[Book]) = {
    Source(data)
      .runWith(
        // TODO settings.withTableName(TableName.valueOf(table))
        HTableStage.sink(settings)
      )
  }

  def find(table: String) = ???

}
