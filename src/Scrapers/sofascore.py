"""
h2h, pregame, votes output a list of their namesake stats
"""

from datetime import datetime, timedelta
from unidecode import unidecode
import cfscrape
import sys
import gevent

sofascore_config = {"days": 3, "all": True}

headers = {"accept-encoding": "gzip, deflate, br"}
no_days = sofascore_config["days"]
scrape_all = sofascore_config["all"]

date_url = "https://api.sofascore.com/api/v1/sport/football/scheduled-events/{date}"
vote_url_format = "https://api.sofascore.com/api/v1/event/{_id}/votes"
pregame_url_format = "https://api.sofascore.com/api/v1/event/{_id}/pregame-form"
h2h_url_format = "https://api.sofascore.com/api/v1/event/{_id}/h2h"
odds_url_format = "https://api.sofascore.com/api/v1/event/{_id}/provider/1/winning-odds"
result_map = {
    "W": 2,
    "D": 1,
    "L": 0
}


def get_sofascore_metadata(cfscraper=None, days=range(no_days), log=sys.stdout):
    """
    returns list of dict containing the metadata of each match on sofascore
    existing cfscraper session can be used
    """
    metadata = []

    if cfscraper is None:
        cfscraper = cfscrape.create_scraper()

    dates = [(datetime.now() + timedelta(days=day)).strftime("%Y-%m-%d")\
        for day in days]
    urls = {date: date_url.format(date=date) for date in dates}

    for date in urls:
        with cfscraper.get(urls[date], headers=headers, timeout=10) as resp:
            if resp.status_code == 200:
                metadata += resp.json()["events"]
            else:
                log.write(f"Sofascore scraper failed on {date} with status code: {resp.status_code}\n")

    metadata = list(map(
        lambda game: {
            "game_time": datetime.fromtimestamp(game["startTimestamp"]),
            "site_id": int(game["id"]),
            "competition": unidecode(game["tournament"]["uniqueTournament"]["name"]),
            "home_team": unidecode(game["homeTeam"]["name"]),
            "away_team": unidecode(game["awayTeam"]["name"]),
            "time_of_collection": None,
            "site": "sofascore"
        },
        filter(lambda game: game["homeScore"] == dict(), metadata)
    ))
    no_dup_metadata = []
    site_ids = []
    for game in metadata:
        if game["site_id"] not in site_ids:
            no_dup_metadata.append(game)
            site_ids.append(game["site_id"])

    return no_dup_metadata


def sofascore_scraper(url_format, name, show_progress=False):
    """
    decorator for scraping sofascore
    since you basically do the same thing for scraping different stats on sofascore
        going through each id, formatting url
    """
    def decorator(func):
        def inner(log=sys.stdout, done_ids=[]):
            """
            done_ids is a list of ids that you *don't* want to be scraped
            useful for data that doesn't really change like h2h
            """
            scraper = cfscrape.create_scraper()
            metadata = get_sofascore_metadata(cfscraper=scraper, log=log)
            metadata = list(filter(lambda game: game["site_id"] not in done_ids, metadata))

            urls = [url_format.format(_id=game["site_id"]) for game in metadata]
            len_metadata = len(metadata)

            data = []

            for game_idx, url in enumerate(urls):
                with scraper.get(url, headers=headers) as resp:
                    if resp.status_code == 200:
                        raw = resp.json()
                        data.append(metadata[game_idx] | func(raw_data=raw) | {"time_of_collection": datetime.utcnow()})
                        if show_progress:
                            log.write(f"Completed {game_idx+1} / {len_metadata} for sofascore {name}")
                            
                    elif resp.status_code == 404:
                        if show_progress:
                            log.write(f"Unable to find {game_idx+1} / {len_metadata} for sofascore {name}")
                    else:
                        log.write(f"Sofascore {name} failed on id {metadata[game_idx]['site_id']} with status code {resp.status_code}")

            return data
        return inner
    return decorator


def add_get_page(url, log, name, list_loc, show_progress=False, game_idx=None, len_metadata=None, _id=None):
    # creating a scraper for every request seems gratuitistly inefficient
    # hopefully it works
    scraper = cfscrape.create_scraper()
    with scraper.get(url, headers=headers) as resp:
        if resp.status_code == 200:
            list_loc.append((game_idx, resp.json()))
            if show_progress:
                log.write(f"Completed {game_idx+1} / {len_metadata} for sofascore {name}\n")
                
        elif resp.status_code == 404:
            if show_progress:
                log.write(f"Unable to find {game_idx+1} / {len_metadata} for sofascore {name}\n")
        else:
            log.write(f"Sofascore {name} failed on id {_id} with status code {resp.status_code}\n")


def async_sofascore_scraper(url_format, name, show_progress=False):
    """
    asynchronously collects the responses unlike the above 
    will probably be *quickly* deprecated by cloufare bot detection
    """
    def decorator(func):
        def inner(log=sys.stdout, done_ids=[]):
            scraper = cfscrape.create_scraper()
            metadata = get_sofascore_metadata(cfscraper=scraper, log=log)
            metadata = list(filter(lambda game: game["site_id"] not in done_ids, metadata))

            urls = [url_format.format(_id=game["site_id"]) for game in metadata]

            raw_data = []
            jobs = [gevent.spawn(add_get_page, url, log, name, raw_data, show_progress=show_progress, game_idx=idx, len_metadata=len(metadata), _id=metadata[idx]["site_id"])
                for idx, url in enumerate(urls)]
            gevent.wait(jobs)
            
            data = [metadata[game_idx] | func(raw_data=raw) | {"time_of_collection": datetime.utcnow()}
                for game_idx, raw in raw_data]

            return data
        return inner
    return decorator


@async_sofascore_scraper(vote_url_format, "votes")
def votes(*args, **kwargs):
    raw_votes = kwargs["raw_data"]
    base_doc = {"home_votes": None, "away_votes": None, "draw_votes": None}
    if "vote" in raw_votes:
        votes = raw_votes["vote"]
        home_votes = away_votes = draw_votes = None
        if "vote1" in votes: home_votes=votes["vote1"]
        if "vote2" in votes: away_votes=votes["vote2"]
        if "voteX" in votes: draw_votes=votes["voteX"]
        base_doc = {
            "home_votes": home_votes, "away_votes": away_votes, "draw_votes": draw_votes
        }
    
    return base_doc
votes.data_type = "votes"
votes.site = "sofascore"

@async_sofascore_scraper(pregame_url_format, "pregame")
def pregame(*args, **kwargs):
    raw_pregame = kwargs["raw_data"]
    home_doc = raw_pregame["homeTeam"]
    away_doc = raw_pregame["awayTeam"]
    base_doc = {"home_rating": None, "home_pos": None, "home_value": None,
        "away_rating": None, "away_pos": None, "away_value": None,
        "home_res": [None for i in range(5)], "away_res": [None for i in range(5)]}

    if "avgRating" in home_doc:
        base_doc["home_rating"] = float(home_doc["avgRating"])
        base_doc["away_rating"] = float(away_doc["avgRating"])

    if "position" in home_doc:
        base_doc["home_pos"] = home_doc["position"]
        base_doc["away_pos"] = away_doc["position"]
    
    if "value" in home_doc:
        base_doc["home_pts"] = int(home_doc["value"])
        base_doc["away_pts"] = int(away_doc["value"])
    
    if "form" in home_doc:
        for team in ["home", "away"]:
            for res_idx, result in enumerate(raw_pregame[f"{team}Team"]["form"]):
                if result not in result_map:
                    continue
                base_doc[f"{team}_res"][res_idx] = result_map[result]
    
    return base_doc
pregame.data_type = "pregame_form"
pregame.site = "sofascore"

@async_sofascore_scraper(h2h_url_format, "h2h")
def h2h(*args, **kwargs):
    raw_h2h = kwargs["raw_data"]
    base_doc = {"home_wins": None, "away_wins": None, "draws": None,
        "home_wins_man": None, "away_wins_man": None, "draws_man": None}
    
    if raw_h2h["teamDuel"] is not None:
        team_doc = raw_h2h["teamDuel"]
        base_doc["home_wins"] = team_doc["homeWins"]
        base_doc["away_wins"] = team_doc["awayWins"]
        base_doc["draws"] = team_doc["draws"]
    
    if raw_h2h["managerDuel"] is not None:
        man_doc = raw_h2h["managerDuel"]
        base_doc["home_wins_man"] = man_doc["homeWins"]
        base_doc["away_wins_man"] = man_doc["awayWins"]
        base_doc["draws_man"] = man_doc["draws"]
    
    return base_doc
h2h.data_type = "h2h"
h2h.site = "sofascore"

@async_sofascore_scraper(h2h_url_format, "odds")
def odds(*args, **kwargs):
    raw_odds = kwargs["raw_data"]
    base_doc = {"home_expected": None, "home_actual": None,
        "home_expected": None, "away_actual": None}
    
    for team_type in ['home', 'away']:
        if team_type in raw_odds:
            base_doc[f"{team_type}_expected"] = raw_odds[team_type]["excepected"]
            base_doc[f"{team_type}_actual"] = raw_odds[team_type]["actual"]
    
    return base_doc
odds.data_type = "odds"
odds.site = "sofascore"
