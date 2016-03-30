package streamserver

import java.lang.Math._

import akka.actor.ActorSystem
import akka.http.Http
import akka.http.model._
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.FlowGraphImplicits._
import akka.stream.scaladsl._
import play.api.libs.json.Json
import play.modules.reactivemongo.json.BSONFormats
import reactivemongo.bson.BSONDocument

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


/**
  * Create a server which uses a custom flow graph to process incoming requests.
  *
  */
object Boot extends App {
  // the actor system to use. Required for flowmaterializer and HTTP.
  // passed in implicit
  implicit val system = ActorSystem("Streams")
  implicit val materializer = FlowMaterializer()
  // start the server on the specified interface and port.
  val serverBinding1 = Http().bind(interface = "localhost", port = 8090)
  serverBinding1.connections.foreach { connection =>
    connection.handleWith(broadCastMergeFlow)
  }


  val bCast = Broadcast[HttpRequest]
  // some basic steps that each retrieve a different ticket value (as a future)
  val step1 = Flow[HttpRequest].mapAsync[String](getTickerHandler("GOOG"))
  val step2 = Flow[HttpRequest].mapAsync[String](getTickerHandler("AAPL"))
  val step3 = Flow[HttpRequest].mapAsync[String](getTickerHandler("MSFT"))
  // We'll use the source and output provided by the http endpoint
  val in = UndefinedSource[HttpRequest]
  val out = UndefinedSink[HttpResponse]
  // when an element is available on one of the inputs, take
  // that one, igore the rest
  val merge = Merge[String]
  // since merge doesn't output a HttpResponse add an additional map step.
  val mapToResponse = Flow[String].map[HttpResponse](
    (inp: String) => HttpResponse(status = StatusCodes.OK, entity = inp)
  )
  // define another flow. This uses the merge function which
  // takes the first available response
  val broadCastMergeFlow = Flow[HttpRequest, HttpResponse]() {
    implicit builder =>
      bCast ~> step1 ~> merge
      in ~> bCast ~> step2 ~> merge ~> mapToResponse ~> out
      bCast ~> step3 ~> merge
      (in, out)
  }

  def convertToString(input: List[BSONDocument]): String = {
    input
      .map(f => convertToString(f))
      .mkString("[", ",", "]")
  }

  def convertToString(input: BSONDocument): String = {
    Json.stringify(BSONFormats.toJSON(input))
  }

  def getTickerHandler(tickName: String)(request: HttpRequest): Future[String] = {
    // query the database
    val ticker = Database.findTicker(tickName)
    Thread.sleep(random() * 1000 toInt)
    // use a simple for comprehension, to make
    // working with futures easier.
    for {
      t <- ticker
    } yield {
      t match {
        case Some(bson) => convertToString(bson)
        case None => ""
      }
    }
  }
}