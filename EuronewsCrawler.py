import scrapy
import json
from scrapy.crawler import CrawlerProcess
import mysql.connector
import schedule
import time 

class EuronewsCrawler(scrapy.Spider):
    
    name='euronews_crawler'
    allowed_domains = ['euronews.com']
    start_urls=['https://www.euronews.com']

    open('articles.json','w').close()
    
    def parse(self,response):

         
        for link in response.xpath('//*[self::h2 or self::h3 or self::h4]/a/@href'):
            yield response.follow(link.get(),callback=self.parse_category)


    def parse_category(self,response):
        products = response.css("div.c-article-content.js-article-content")
        for product in products:
            yield{

                'title':response.css('header h1::text').get().replace('\n','').strip(),
                'article':product.xpath('//div[contains(@class,"c-article-content")]//*[self::h2 or self::p]/text()').getall()
                
            }


process = CrawlerProcess(settings = {
    'FEED_URI': 'articles.json',
    'FEED_FORMAT': 'json'
    
    })
process.crawl(EuronewsCrawler)
process.start()



##### Connect and insert scraped data into database #####
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database = "crawled_news_database"

)
mycursor = mydb.cursor()

sql="INSERT INTO articles(Title, Article) SELECT %s, %s FROM DUAL WHERE NOT EXISTS(SELECT 1 FROM articles WHERE Title = %s) LIMIT 1;"




file = open('articles.json')
data = json.load(file)

for j in data:
    try:
        text = ''
        for i in j['article']:
            text = text + i
    
        title = j['title']
        article = text
        value=(title,article,title)
        mycursor.execute(sql,value)
        mydb.commit()
    except :
        continue


file.close()