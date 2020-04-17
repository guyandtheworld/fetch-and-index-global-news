# Aggregation Microservices

## 1. News Aggregation : Data Scraping
This application tracks a news based on the keyword we're inputting. It has two modes while tracking a particular keyword. It fetches articles based on the time filters passed and the keywords passed to fetch news. The news for a particular entity is scraped based on the alias table. After aggregating, this service dumps the raw file into google storage.

* historic: fetches the past one year of news articles for the particular entity
* daily: once we start monitoring a company, we fetch and store the news daily for that entity

### Databases
Right now, noel the news aggregator monitors two open databases.

* GDELT: We can fetch news from GDELT on multiple languages, regions and we can get upto 250 articles per request. If we time that request properly, we can get all of it.

* Google News: Google News doesn't provide any official APIs. All we have is access to the RSS links where we monitor multiple regions for a particular keyword using URLS.
To load a new database, write a class and put it in workers/sources. The class should have the following format:

```
class Source:
    def __init__(self):
        pass

    def history(self):
        pass

    def daily(self):
        pass

    def write_data(self):
        pass
```

Subscribes to: `news_aggregator`
Publishes to: `index_articles`

## 2. News Indexing

News indexing listens to the the `index_articles` subscription and normalizes the format and inputs the rows into our Cloud SQL PostgreSQL database.

Subscribes to: `index_articles`
Publishes to: None

## 3. Body Extraction

Microservice to extract body from the new data sources.

Subscribes to: `body_extractor`
Publishes to: None
