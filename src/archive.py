"""
Scrapes archived data from betexplorer and sofascore
matches using match.py
uses sofascore scraper for sofascore scraping
betexplorer outcome scraper for betexplorer metadata scraping 
and there is an empty stub for archive odds data scraping
FUN
"""

import gevent 
from gevent import monkey; monkey.patch_all()

from datetime import datetime, timedelta
import json

from match import group_by_match
from Scrapers import sofascore, betexplorer


# need limit on number of requests sent at a time
# else connection timeout errors get real fucky
NO_SCRAPERS = 1
START_DATE = datetime.now()
LAST_DATE = datetime.strptime("01,08,2022", "%d,%m,%Y")


def save_file_stub(data, date, **_kw):
    with open(rf"test/scrape_fun/{date.strftime('%d-%m-%Y')}.json", "w") as fp :
        json.dump(data, fp, indent=4)


def scrape_main(scraper_idx, return_stub):
    """
    cycle back through dates 
    return stub is a function that handles the fully processed json
    e.g. put into database, save to file etc.
    """
    scrape_date = START_DATE - timedelta(days=scraper_idx)
    while scrape_date > LAST_DATE:
        betexplorer_data = betexplorer.outcomes()
        print("beep")
        scrape_data = betexplorer_data

        return_stub(scrape_data, scrape_date)

        scrape_date -= timedelta(days=NO_SCRAPERS)
    
    print(f"Scraper no. {scraper_idx} finished")


def main():
    jobs = [gevent.spawn(scrape_main, scraper_idx, save_file_stub) for scraper_idx in range(NO_SCRAPERS)]
    gevent.wait(jobs)

    print("----All scrapers completed-----")


if __name__ == "__main__":
    main()