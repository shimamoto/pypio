package api

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._

class Route(uid: Long) {

  val route =
    path("") {
      get {
        complete(s"Hello from $uid")
//        complete(config.getString("application.api.hello-message"))
      }
    } ~
    path("orders") {
      get {
        complete(StatusCodes.OK)
      } ~
      post {
        entity(as[String]) { str =>
          complete(HttpEntity(ContentTypes.`application/json`, s"""{ "msg" : "Order received: $str" }"""))
        }
      }
    }

}
