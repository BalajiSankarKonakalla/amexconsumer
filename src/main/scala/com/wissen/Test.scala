package com.wissen

import com.wissen.moketypes.Transaction

object Test {
  def main(args: Array[String]): Unit = {
    val s = """{
              | "id": "d190180a-6217-47ae-8c86-24b72c0c5214",
              | "mcc": "Other",
              | "cardNumber": 7036098331879721730,
              | "customerName": "Noe Bolgar",
              | "cardType": "Corporate",
              | "issuingBank": "Kiwi Bank",
              | "transactionAmount": 400.0,
              | "location": "Neddy Eggerton",
              | "acquiringBank": "Chase"
              |}""".stripMargin

    println(
      Transaction.fromJson(s)
    )

  }
}
