from datetime import timedelta, datetime

# Prefect imports
from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule

# PySpark imports
from pyspark.sql import functions as F
from pyspark.sql import Window as Window
from pyspark.sql import types
from pyspark.context import SparkContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# Web scraping imports
from urllib.request import urlopen
from bs4 import BeautifulSoup

# Classes
from classes.article import Article


@task(max_retries=1, retry_delay=timedelta(seconds=1))
def extract_efarsas_data(feed_url, debug=False):

    # A list of links for articles that were on feed's first page
    articles_links = get_articles_links_from_feed(feed_url)
    articles_links.pop(0)  # the first one isn't actually a fact check article
    if debug:
        print('---- All articles links found ----')
        print(articles_links)

    # # Now that we have a list of article links, we should gather data from each article link
    # articles = []
    # for article_link in articles_links:
    #     article = get_article_from_url(article_link)
    #     articles.append(article)

    # if debug:
    #     print('---- All articles ----')
    #     print(articles)

    # # Now we create an list that contains articles as lists. Using this we will be able to create a spark dataframe
    # articles_as_list = [art.toList() for art in articles]

    # columns_schema = articles[0].attributes()

    # print(" <<< G1 - FATO OU FAKE >>> - # Extracted")
    # return articles_as_list_to_dataframe(articles_as_list, columns_schema)


def get_articles_links_from_feed(feed_url):

    html = urlopen(feed_url)
    bs = BeautifulSoup(html, 'html.parser')

    # Getting only articles listed on feed with type 'materia'
    articles = bs.find_all(
        'h3', attrs={'class': 'entry-title td-module-title'})

    # Then I get all links based on the class 'feed-post-link'
    articles_links = []
    for article in articles:
        link = article.find(
            'a').get('href')
        articles_links.append(link)

    return articles_links


def get_article_from_url(article_url):

    raw_article_html = urlopen(article_url)
    bs_article_html = BeautifulSoup(raw_article_html, 'html.parser')

    # > Title
    article_title = bs_article_html.find(
        'h1', attrs={'class': 'content-head__title'}).text

    # > Subtitle
    article_subtitle = bs_article_html.find(
        'h2', attrs={'class': 'content-head__subtitle'}).text

    # > Text
    # This body has multiple divs that contains all paragraphs, text,
    # video and ads from the article
    article_body = bs_article_html.find(
        'article', attrs={'itemprop': 'articleBody'})

    # Filtering only text divs and concatenating into a single string
    article_text = article_body.find_all(
        'div', attrs={'class': 'mc-column content-text active-extra-styles'}
    )
    article_text = list(map(lambda column: column.text, article_text))
    article_text = ''.join(article_text)
    article_text = article_text.replace("\"", "\"\"")
    article_text = "\n" + article_text + "\""

    # > Publish Date
    article_publish_date = bs_article_html.find(
        'time', attrs={'itemprop': 'datePublished'})['datetime']

    # > Modified Date
    article_modified_date = bs_article_html.find(
        'time', attrs={'itemprop': 'dateModified'})['datetime']

    # > Author
    article_author = bs_article_html.find('div', attrs={
        'class': 'content-publication-data'}).find('span', attrs={'itemprop': 'author'}).meta["content"]

    # Creates and returns a new article object
    article = Article(article_title, article_subtitle, article_text,
                      article_publish_date, article_modified_date, article_author, 'G1')

    return article


def articles_as_list_to_dataframe(articles_as_list, columns_schema):
    spark = (SparkSession.builder.master('local').getOrCreate())
    return spark.createDataFrame(articles_as_list, columns_schema)


# Called when running the file directly
if __name__ == "__main__":
    feed_url = "https://www.e-farsas.com/secoes/falso-2"
    extract_efarsas_data.run(feed_url, debug=True)
