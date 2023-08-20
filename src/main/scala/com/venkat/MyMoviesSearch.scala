package com.venkat

import scala.collection.mutable

object MyMoviesSearch {
  //var currentYear = 2020

  private val seriesNames = mutable.Map(
    "The Witcher" -> 2019,
    "Unbelievable" -> 2019,
    "Tiger King" -> 2020,
    "The Stranger" -> 2020)

  /*private def searchYear(year: Int) =
    for(name <- seriesNames; if name._2 == year)
      yield name

  private def searchSeries(keyword: String) =
    for (name <- seriesNames; if name._1.contains(keyword))
      yield name

  private def regSearchSeries(keyword: String) =
    for (name <- seriesNames; if name._1.matches(keyword))
      yield name


  def main(args: Array[String]): Unit = {
    println(searchYear(currentYear))
    println(searchSeries("The"))
  }*/

 /* private def namesMatching(matcher: String => Boolean) =
    for(name <- seriesNames; if matcher(name))
      yield name

  def namesStartWith(keyword: String) =
    namesMatching(_.startsWith(keyword))

  def nameContains(keyword: String) =
    namesMatching(_.contains(keyword))

  def nameRegex(keyword: String) =
    namesMatching(_.matches(keyword))

  def main(args: Array[String]): Unit = {
    println(namesMatching("The"))
  }*/

}
