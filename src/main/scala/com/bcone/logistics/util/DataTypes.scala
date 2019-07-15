package com.bcone.logistics.util

object DataTypes {
    def typeMapper(t:String):String = {
    t match {
      case "timestamp" => "date"
      case "int" => "number"
      case "bigint" => "number"
      case "double" => "number"
      case "string" => "text"
      case s if s.contains("decimal") => "number"
    }
  }

}
