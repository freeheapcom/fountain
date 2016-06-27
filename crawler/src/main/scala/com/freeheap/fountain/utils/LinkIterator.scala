package com.freeheap.fountain.utils

import java.util.regex.Pattern

import edu.uci.ics.crawler4j.crawler.{CrawlConfig, Page}
import edu.uci.ics.crawler4j.fetcher.{PageFetcher, PageFetchResult}
import edu.uci.ics.crawler4j.parser.{Parser, HtmlParseData, ParseData}
import edu.uci.ics.crawler4j.url.WebURL
import org.apache.http.HttpStatus

/**
 * Created by minhdo on 1/22/16.
 */
class LinkIterator extends Iterator[Page] {
  val FILTERS: Pattern = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g" + "|png|tiff?|mid|mp2|mp3|mp4" + "|wav|avi|mov|mpeg|ram|m4v|pdf" + "|rm|smil|wmv|swf|wma|zip|rar|gz))$")

  var temPage: Page = null
  val config: CrawlConfig = new CrawlConfig
  config.addAcceptedLanguage("en")
  config.setIncludeBinaryContentInCrawling(false)
  config.setPolitenessDelay(1000)
  val parser: Parser = new Parser(config)
  val pageFetcher: PageFetcher = new PageFetcher(config)

  override def hasNext: Boolean = {
      true
  }

  override def next(): Page = {
    var url = ""
    do {
      url = DataUtils.getItem(DataUtils.SOURCE)
      if (shouldVisit(url)) {
        temPage = processUrl(url)
      } else {
        temPage = null
      }
    } while (temPage == null)

    import scala.collection.JavaConversions._
    for( s <- temPage.getParseData.getOutgoingUrls) {
        DataUtils.setItem(DataUtils.SOURCE, s.getURL)
    }
    DataUtils.setItem(DataUtils.TARGET, url)
    temPage
  }

  private def shouldVisit(url: String) : Boolean = {
    val href: String = url.toLowerCase
    if (FILTERS.matcher(href).matches) {
      return false
    }

    true
  }

  private def processUrl(url: String) : Page = {
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
          println("Language: " + page.getLanguage)
        }
      } else {
        println("Couldn't parse the content of the page.")
      }
    } else {
      println("Couldn't fetch the content of the page.")
    }
    println("==============")
    page
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
        if (config.getAcceptedLanguages.contains(page.getLanguage))
           return page
        return null
      } else if (fetchResult.getMovedToUrl != null) {
        return download(fetchResult.getMovedToUrl)
//        println("fetchResult.getStatusCode is not 200: " + fetchResult.getStatusCode)
//        println("fetchResult.getMovedToUrl11: " + fetchResult.getMovedToUrl)
//        curURL.setURL(fetchResult.getMovedToUrl)
//        if (fetchResult.getStatusCode == HttpStatus.SC_OK) {
//          val page: Page = new Page(curURL)
//          fetchResult.fetchContent(page)
//          parser.parse(page, curURL.getURL)
//          return page
//        } else {
//          println("fetchResult.getStatusCode2 : " + fetchResult.getStatusCode)
//        }
      } else {
        println("fetchResult.getStatusCode : " + fetchResult.getStatusCode)
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
