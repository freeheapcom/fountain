package com.freeheap.fountain.test

import edu.uci.ics.crawler4j.crawler.{CrawlConfig, Page}
import edu.uci.ics.crawler4j.fetcher.{PageFetchResult, PageFetcher}
import edu.uci.ics.crawler4j.parser.{HtmlParseData, ParseData, Parser}
import edu.uci.ics.crawler4j.url.WebURL
import org.apache.http.HttpStatus

object CrawlerApp {
  val config: CrawlConfig = new CrawlConfig
  val parser: Parser = new Parser(config)
  val pageFetcher: PageFetcher = new PageFetcher(config)

  def main(args: Array[String]) {
    //processUrl("https://en.wikipedia.org/wiki/Main_Page/");
    //processUrl("https://www.yahoo.com/");
    processUrl("http://www.bumblebeehub.com/")
  }

  def processUrl(url: String) {
    println("Processing: ", url)
    val page: Page = download(url)
    if (page != null) {
      val parseData: ParseData = page.getParseData
      if (parseData != null) {
        if (parseData.isInstanceOf[HtmlParseData]) {
          val htmlParseData: HtmlParseData = parseData.asInstanceOf[HtmlParseData]

          println("Title: " + htmlParseData.getTitle)
          println("Text length: " + htmlParseData.getText.length)
          println("Html length: " + htmlParseData.getHtml.length)
        }
      }
      else {
        println("Couldn't parse the content of the page.")
      }
    } else {
      println("Couldn't fetch the content of the page.")
    }
    println("==============")
  }

  private def download(url: String): Page = {
    val curURL: WebURL = new WebURL
    curURL.setURL(url)
    var fetchResult: PageFetchResult = null
    try {
      fetchResult = pageFetcher.fetchPage(curURL)
      if (fetchResult.getStatusCode == HttpStatus.SC_OK) {
        val page: Page = new Page(curURL)
        fetchResult.fetchContent(page)
        parser.parse(page, curURL.getURL)
        return page
      }
    }
    catch {
      case e: Exception => {
        println("Error occurred while fetching url: " + curURL.getURL, e)
      }
    } finally {
      if (fetchResult != null) {
        fetchResult.discardContentIfNotConsumed
      }
    }
    return null
  }
}