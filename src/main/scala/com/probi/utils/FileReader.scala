package com.probi.utils

import scala.io.Source

object FileReader {

  def main(args: Array[String]): Unit = {


    val fileContents = Source.fromFile("feedurls.csv").getLines().mkString(",")


    print(fileContents)



  }


}
