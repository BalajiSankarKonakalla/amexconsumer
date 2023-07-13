package com.wissen.moketypes

import org.json4s._
import org.json4s.jackson.JsonMethods._


case class Transaction(
                        id: String,
                        mcc: String,
                        cardNumber: Long,
                        customerName: String,
                        cardType: String,
                        issuingBank: String,
                        transactionAmount: Double,
                        location: String,
                        acquiringBank: String
                      ){
  def toJson: String = {
    s"""{
       | "id": "$id",
       | "mcc": "$mcc",
       | "cardNumber": $cardNumber,
       | "customerName": "$customerName",
       | "cardType": "$cardType",
       | "issuingBank", "$issuingBank",
       | "transactionAmount": $transactionAmount,
       | "location": "$location",
       | "acquiringBank": "$acquiringBank"
       |}""".stripMargin
  }
}

object Transaction{

  def fromJson(s: String): Transaction = {
    implicit val formats = org.json4s.DefaultFormats
    val transactionMap = parse(s).extract[Map[String, Any]]

    new Transaction(
      id = transactionMap("id").asInstanceOf[String],
      mcc = transactionMap("mcc").asInstanceOf[String],
      cardNumber = transactionMap("cardNumber").asInstanceOf[BigInt].toLong,
      customerName = transactionMap("customerName").asInstanceOf[String],
      cardType = transactionMap("cardType").asInstanceOf[String],
      issuingBank = transactionMap("issuingBank").asInstanceOf[String],
      transactionAmount = transactionMap("transactionAmount").asInstanceOf[Double],
      location = transactionMap("location").asInstanceOf[String],
      acquiringBank = transactionMap("acquiringBank").asInstanceOf[String]
    )
  }

}
