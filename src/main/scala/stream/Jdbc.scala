package stream

import akka.stream.Materializer
import akka.stream.alpakka.slick.scaladsl._
import akka.stream.scaladsl.{Sink, Source}
import slick.jdbc.GetResult

class Jdbc(implicit materializer: Materializer) {
  private implicit val session = SlickSession.forConfig("slick-postgres")
  private implicit val bookResult = GetResult(r => Book(r.nextString, r.nextString))

  import session.profile.api._

  def close() = {
    session.close()
  }

  def start(table: String) = {
    val f = session.db.run(sqlu"""CREATE TABLE IF NOT EXISTS #$table(NO VARCHAR(50) PRIMARY KEY, TITLE VARCHAR(50) NOT NULL)""")
    concurrent.Await.result(f, concurrent.duration.Duration.Inf)
  }

  def write(table: String, data: List[Book]) = {
    Source(data)
      .runWith(
        Slick.sink(b => sqlu"INSERT INTO #$table VALUES(${b.no}, ${b.title})")
      )
  }

  def find(table: String) = {
    Slick
      .source(sql"SELECT NO, TITLE FROM #$table".as[Book])
      .runWith(Sink.seq)
  }

}
