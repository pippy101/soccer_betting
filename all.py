"""
Example/test:
scrapes all the data from all the sites
"""
import gevent 
from gevent import monkey; monkey.patch_all()
import json

from src import match
from src.Scrapers import (
    betexplorer,
    forebet,
    odds_scrapers,
    sofascore
)

def get_data(_func, list_loc):
    data = _func()
    print(f"{_func.__name__}: {len(data):,}")
    list_loc += data

games = []
functions = [betexplorer.outcomes, forebet.predictions, odds_scrapers.get_all_data,
    sofascore.h2h, sofascore.votes, sofascore.pregame]
jobs = [gevent.spawn(get_data, _func, games) for _func in functions]
gevent.wait(jobs)

json.dump(games, open(r"test/all_data.json", "w"), indent=4)
matched = match.group_by_match(games, -1)
json.dump(matched, open(r"test/all_data_grouped.json", "w"), indent=4)
print(len(matched))
