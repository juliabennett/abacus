package abacus.streams.twitter

import java.time.Instant
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.io.Source
import scala.util.Random
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._

/**
  * Request to Twitter API handling authorization.
  *
  * @param requestParameters API request parameters
  * @param baseUrl Endpoint URL, defaults to filter endpoint
  * @param usePostRequest Determines if request type is GET or POST, defaults to False
  */
case class TwitterRequest(
  requestParameters: Map[String, String],
  baseUrl: String = "https://stream.twitter.com/1.1/statuses/filter.json",
  usePostRequest: Boolean = false) {

  /* Returns request object with fresh authorization header. */
  def request: HttpRequest = {
    HttpRequest(
      if (usePostRequest) POST else GET,
      uri = baseUrl + "?" + glueParams(requestParameters),
      headers = List(headers.RawHeader("Authorization", generateAuthString)))
  }

  // Parse resource file to get authorization values
  private val secrets: Map[String, String] = Source.fromResource("secrets.txt")
    .getLines
    .map(_.split("="))
    .map(pair => (pair(0), pair(1)))
    .toMap

  /* Returns new Authorization header string with current time information. */
  private def generateAuthString: String = {
    val oauthParameters = Map(
      "oauth_consumer_key" -> secrets("consumerKey"),
      "oauth_nonce" -> generateNonce,
      "oauth_signature_method" -> "HMAC-SHA1",
      "oauth_timestamp" -> Instant.now.getEpochSecond.toString,
      "oauth_token" -> secrets("token"),
      "oauth_version" -> "1.0")

    val signatureParam = ("oauth_signature", generateSignature(oauthParameters))

    (oauthParameters + signatureParam)
      .map(tup => tup._1 + "=\"" + percentEncode(tup._2) + "\"")
      .mkString("OAuth ", ", ", "")
  }

  /* Returns oAuth signature. */
  private def generateSignature(oauthParameters: Map[String, String]): String = {
    val queryString = glueParams(requestParameters ++ oauthParameters)
    val signatureBaseString = List(
        if (usePostRequest) "POST" else "GET",
        percentEncode(baseUrl),
        percentEncode(queryString))
      .mkString("&")

    val consumerSecret = percentEncode(secrets("consumerSecret"))
    val tokenSecret = percentEncode(secrets("tokenSecret"))

    hmacSha1(
      consumerSecret + "&" + tokenSecret,
      signatureBaseString)
  }

  /* Returns percent encoded string. */
  private def percentEncode(s: String): String = {
    val symbols = Set('-', '.', '_', '~')
    val letters = ('a' to 'z').toSet ++ ('A' to 'Z').toSet
    val numbers = ('0' to '9').toSet
    val whiteList = symbols ++ letters ++ numbers

    val translations = Map(
      ' ' -> "%20",
      '/' -> "%2F",
      '+' -> "%2B",
      '=' -> "%3D",
      '%' -> "%25",
      '!' -> "%21",
      ',' -> "%2C",
      ':' -> "%3A",
      '&' -> "%26",
      '#' -> "%23")

    s.flatMap { char =>
      if (whiteList.contains(char)) char.toString else translations(char)
    }
  }

  /* Returns sorted query string constructed from URL parameters. */
  private def glueParams(parameters: Map[String, String]) = {
    parameters.toList
      .map(tup => percentEncode(tup._1) + "=" + percentEncode(tup._2))
      .sorted
      .mkString("&")
  }

  /* Returns random alphanumeric string with 25 characters. */
  private def generateNonce: String = {
    val alphabet = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).map(_.toString)
    (1 to 25)
      .map(_ => Random.nextInt(alphabet.length))
      .map(alphabet(_))
      .reduce(_ + _)
  }

  /* Returns HMAC using SHA1 hash function. */
  private def hmacSha1(key:String, data:String): String = {
    val hmacSha1Secret = new SecretKeySpec(key.getBytes, "HmacSHA1")
    val mac = Mac.getInstance("HmacSHA1")
    mac.init(hmacSha1Secret)
    Base64.getEncoder.encodeToString(mac.doFinal(data.getBytes))
  }

}
