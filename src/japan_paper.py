import gevent 
from gevent import monkey; monkey.patch_all()

import time
from datetime import timedelta

import better_match as match
from Scrapers import odds

odds_scrapers = [odds.neds, odds.tab, odds.bluebet,
    odds.ps3838, odds.palmerbet,
    odds.onex, odds.interwetten,
    odds.playup, odds.betfair]


def get_data(_func, list_loc):
    start = time.time()
    data = _func()
    for game in data:
        game["data_type"] = _func.data_type
        game["site"] = _func.site
    time_taken = time.time()-start
    print(f"{_func.site} {_func.data_type}: {len(data):,} in {timedelta(seconds=int(time_taken))}")
    list_loc += data


def get_odds():
    games_list = []
    jobs = [gevent.spawn(get_data, scraper, games_list) for scraper in odds_scrapers]
    gevent.wait(jobs)
    no_games = len(games_list)
    matched_games = match.group_by_match(games_list, -1)
    print(f"Average of {round(no_games/len(matched_games),2)} bookies per match")
    return concat_matches(matched_games)


def concat_matches(matches, concat_fields=["home_odds", "draw_odds", "away_odds", "home_volume", "draw_volume", "away_volume"],
    ignore_fields=["home_active", "away_active", "draw_active"]):
    concat = dict()
    for _id, games in matches.items():
        concat[_id] = {field: item for field, item in games[0].items() if field not in concat_fields + ignore_fields}
        concat[_id]["no_bookies"] = len(games)
        for game in games:
            for field, item in game.items():
                if sum([field in concat_field for concat_field in concat_fields]):
                    concat[_id][f"{game['site']}_{field}"] = item

    return concat

if __name__ == "__main__":
    import json

    json.dump(get_odds(), open(r"test/japan_test_new_matching.json", "w"), indent=4, default=str)
