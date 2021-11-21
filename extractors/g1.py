from datetime import timedelta, datetime

# Prefect imports
from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule

# Web scraping imports
from urllib.request import urlopen
from bs4 import BeautifulSoup


@task(max_retries=1, retry_delay=timedelta(seconds=1))
def extract_g1_data(url, debug=False):
    # Getting all articles links
    html = urlopen(url)
    bs = BeautifulSoup(html, 'html.parser')

    # >>> At that page, all articles contains the class 'bastian-feed-item'
    articles = bs.find_all(
        'div', attrs={'class': 'bastian-feed-item'})
    # >>> Then I get all links based on the class 'feed-post-link'
    articlesLinks = []
    for article in articles:
        link = article.find('a', attrs={'class': 'feed-post-link'}).get('href')
        articlesLinks.append(link)
    if debug:
        print(articlesLinks)

    # >>> Now that we have a list of article links, we should gather data from each article link

    print(" <<< G1 - FATO OU FAKE >>> - # Extracted")
    return "g1"


# TO DO: Remove this function from here, it should have it's own file
@task(max_retries=1, retry_delay=timedelta(seconds=1))
def load_g1_raw_data(g1_raw_data):
    print("loaded g1 raw data")


# Called when running the file directly
if __name__ == "__main__":
    g1_url = "https://g1.globo.com/fato-ou-fake/"
    extract_g1_data.run(g1_url, debug=True)
