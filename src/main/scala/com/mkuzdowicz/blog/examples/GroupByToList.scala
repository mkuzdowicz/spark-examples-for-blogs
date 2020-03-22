package com.mkuzdowicz.blog.examples

import org.apache.spark.sql.functions.{collect_list, struct}
import org.apache.spark.sql.{Dataset, SparkSession}

case class CryptoToken(ticker: String, count: Double)

case class CryptoInvestor(investorId: String, countryCode: String, tokens: List[CryptoToken])

case class CryptoTokenPriceReport(ticker: String, priceInUSD: Double)

case class CryptoTokenWithCurrentValue(ticker: String, count: Double, priceInUSD: Double)

case class CryptoInvestorsReport(investorId: String, countryCode: String, tokensWithCurrentPricing: List[CryptoTokenWithCurrentValue])


object GroupByToList extends App {

  val appName = "GroupByToListExampleApp"

  val spark = SparkSession.builder()
    .master("local")
    .appName(appName)
    .getOrCreate()

  import spark.implicits._

  val investorsPortfolios: Dataset[CryptoInvestor] = List(
    CryptoInvestor("kyc-investor1", "USA", List(
      CryptoToken("btc", 0.5),
      CryptoToken("eth", 5)
    )),
    CryptoInvestor("kyc-investor2", "UK", List(
      CryptoToken("ltc", 1.5),
      CryptoToken("eth", 3)
    ))
  ).toDS()

  val tokensPriceReport: Dataset[CryptoTokenPriceReport] = List(
    CryptoTokenPriceReport("btc", 8000.1),
    CryptoTokenPriceReport("eth", 123.0),
    CryptoTokenPriceReport("ltc", 51.2)
  ).toDS()

  case class CryptoInvestorTmpRow(investorId: String, countryCode: String, ticker: String, count: Double)

  val investorsAndPriceReportJoin = investorsPortfolios.flatMap { investor =>
    import investor._
    tokens.map { token =>
      import token._
      CryptoInvestorTmpRow(investorId, countryCode, ticker, count)
    }
  }.as[CryptoInvestorTmpRow].join(tokensPriceReport, "ticker")

  investorsAndPriceReportJoin.show()

  val result = investorsAndPriceReportJoin.groupBy("investorId", "countryCode").agg(
    collect_list(struct("ticker", "count", "priceInUSD")) alias "tokensWithCurrentPricing"
  ).as[CryptoInvestorsReport]

  result.show(truncate = false)
}
