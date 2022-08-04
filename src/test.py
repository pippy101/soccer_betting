"""
Example/test:
scrapes all the data from all the sites
"""
import gevent 
from gevent import monkey; monkey.patch_all()
from datetime import timedelta
import json
import time

from Scrapers import betexplorer, forebet, sofascore, odds
import match

def get_data(_func, list_loc):
    start = time.time()
    data = _func()
    for game in data:
        game["data_type"] = _func.data_type
        game["site"] = _func.site
    time_taken = time.time()-start
    print(f"{_func.site} {_func.data_type}: {len(data):,} in {timedelta(seconds=int(time_taken))}")
    list_loc += data

games = []
functions = [betexplorer.outcomes, forebet.predictions,
        sofascore.votes, sofascore.h2h, sofascore.pregame, sofascore.odds,
        odds.neds, odds.tab, odds.bluebet,
        odds.ps3838, odds.palmerbet,
        odds.onex, odds.interwetten,
        odds.playup, odds.betfair]

jobs = [gevent.spawn(get_data, _func, games) for _func in functions]
gevent.wait(jobs)

json.dump(games, open(r"test/all_data.json", "w"), indent=4)
matched = match.group_by_match(games, -1)
sorted_keys = sorted(matched, key=lambda k: len(matched[k]), reverse=True)
sorted_matches = {k:matched[k] for k in sorted_keys}
json.dump(sorted_matches, open(r"test/all_data_grouped.json", "w"), indent=4)
print(f"Found {len(matched):,} unique games")
