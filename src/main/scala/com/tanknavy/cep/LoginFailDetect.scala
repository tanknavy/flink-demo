package com.tanknavy.cep

/**
 * Author: Alex Cheng 12/5/2020 3:54 PM
 */
//账户连续登陆失败检车
case class LoginEvent(userId:Long, ip:String, eventType:String,  eventTime:Long)
case class LoginWarning(userId:Long, firstFailTime:Long, lastFailTime:Long, warningMsg: String)

object LoginFailDetect {
  def main(args: Array[String]): Unit = {

  }
}
