package stream

import akka.stream.Materializer
import akka.stream.alpakka.elasticsearch.WriteMessage
import akka.stream.alpakka.elasticsearch.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import spray.json._
import DefaultJsonProtocol._

class Elasticsearch(implicit materializer: Materializer) {
  private implicit val client = RestClient.builder(new HttpHost("localhost", 9200)).build()
  private implicit val format = jsonFormat2(Book)

  def close() = {
    client.close()
  }

  def start(index: String) = {
    client.performRequest(
      "HEAD",
      s"/$index",
      java.util.Collections.emptyMap[String, String]).getStatusLine.getStatusCode match {
        case 404 =>
          client.performRequest(
            "PUT",
            s"/$index",
            java.util.Collections.emptyMap[String, String])
        case _ =>
    }
  }

  def write(index: String, data: List[Book]) = {
    Source(data.map { b =>
      WriteMessage.createUpsertMessage(id = b.no, source = b)
    })
    .runWith(
      ElasticsearchSink.create[Book](index, "_doc")
    )
  }

  def find(index: String) = {
    ElasticsearchSource.typed[Book](
      index,
      "_doc",
      """{"match_all": {}}"""
    ).map { message =>
      message.source
    }
    .runWith(Sink.seq)
  }

}

case class Book(no: String, title: String)
