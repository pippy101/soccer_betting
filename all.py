"""
Example/test:
scrapes all the data from all the sites
"""
import gevent 
from gevent import monkey; monkey.patch_all()
from datetime import timedelta
import json
import time

from src.Scrapers import betexplorer, forebet, sofascore, odds
from src import match

def get_data(_func, list_loc):
    start = time.time()
    data = _func()
    time_taken = time.time()-start
    print(f"{_func.name}: {len(data):,} in {timedelta(seconds=int(time_taken))}")
    list_loc += data

games = []
functions = [betexplorer.outcomes, forebet.predictions,
        sofascore.votes, sofascore.h2h, sofascore.pregame,
        odds.neds, odds.tab, odds.bluebet,
        odds.ps3838, odds.palmerbet,
        odds.onex, odds.interwetten,
        odds.playup, odds.betfair]

jobs = [gevent.spawn(get_data, _func, games) for _func in functions]
gevent.wait(jobs)

json.dump(games, open(r"test/all_data.json", "w"), indent=4)
matched = match.group_by_match(games, -1)
json.dump(matched, open(r"test/all_data_grouped.json", "w"), indent=4)
print(f"Found {len(matched):,} unique games")
