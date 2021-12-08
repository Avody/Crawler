import scrapy
import json
from scrapy.crawler import CrawlerProcess
import mysql.connector
import schedule
import time 

##### Scrape site #####
class BbcCrawler(scrapy.Spider):
    
    name='bbc_crawler'
    allowed_domains = ['bbc.com']
    start_urls=['https://bbc.com']

    open('articles.json','w').close()
    
    def parse(self,response):

         
        for link in response.xpath('//a/@href'):
            one_link = link.get()
            if "https" not in one_link:
                yield response.follow(link.get(),callback=self.parse_category)
            
            else:
                yield scrapy.Request(one_link,callback=self.parse_category)
                

    def parse_category(self,response):
        
        try:
            yield{

                'title':response.css('header h1::text').get().replace('\n','').strip(),
                'article':response.xpath('//div[@data-component="text-block" or @class="qa-story-body story-body gel-pica gel-10/12@m gel-7/8@l gs-u-ml0@l gs-u-pb++"]//*[self::p or self::b or self::a or self::span]/text()').extract()
            }

        except:
            
            yield{

                'title':response.xpath('//div[contains(@class,"article__intro")]/text()').get().strip(),
                'article':response.xpath('//div[contains(@class,"body-text-card__text")]//*[self::p or self::b or self::a]/text()').extract()
                    
            }

process = CrawlerProcess(settings = {
    'FEED_URI': 'articles.json',
    'FEED_FORMAT': 'json'
    
    })


process.crawl(BbcCrawler)
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