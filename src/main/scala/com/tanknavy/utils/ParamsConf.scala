package com.tanknavy.utils

import com.typesafe.config.ConfigFactory

/**
 * Author: Alex Cheng 11/29/2020 7:13 PM
 */

//参数配置类方法一，使用typesafe的ConfigFactory(scala推荐,因为有lazy,调用时读取资源文件)从resources下面读取并解析参数
object ParamsConf {

  private lazy val config = ConfigFactory.load() //懒执行,加载资源文件, load返回Config

  val url = config.getString("jdbc.url")
  val user = config.getString("dbuser")
  val password = config.getString("password")

}
