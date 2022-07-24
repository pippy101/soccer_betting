"""
h2h, pregame, votes output a list of their namesake stats
"""

from datetime import datetime, timedelta
from unidecode import unidecode
import requests
import cfscrape
import sys

sofascore_config = {"days": 3, "all": True}

headers = {"accept-encoding": "gzip, deflate, br"}
no_days = sofascore_config["days"]
scrape_all = sofascore_config["all"]

date_url = "https://api.sofascore.com/api/v1/sport/football/scheduled-events/{date}"
vote_url_format = "https://api.sofascore.com/api/v1/event/{_id}/votes"
pregame_url_format = "https://api.sofascore.com/api/v1/event/{_id}/pregame-form"
h2h_url_format = "https://api.sofascore.com/api/v1/event/{_id}/h2h"
result_map = {
    "W": 1,
    "D": 0,
    "L": -1
}


def future_proc_func(metadata):
    """
    filters out all games that have finished or are in play
        ~only keeps games that are going to happen in the future~
    extracts the necessary info
    """
    metadata = filter(lambda game: game["homeScore"] == dict(), metadata)
    # python go brrrrr
    metadata = list(map(
        lambda game: {
            "match_datetime": game["startTimestamp"],
            "site_id": str(game["id"]),
            "competition": unidecode(game["tournament"]["uniqueTournament"]["name"]),
            "home_team": unidecode(game["homeTeam"]["name"]),
            "away_team": unidecode(game["awayTeam"]["name"]),
            "time_of_collection": None,
            "site": "sofascore"
        },
        metadata
    ))
    return metadata


def get_sofascore_metadata(cfscraper=None, days=range(no_days), process_func=future_proc_func, log=sys.stdout):
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
        try:
            with cfscraper.get(urls[date], headers=headers, timeout=10) as resp:
                if resp.status_code == 200:
                    metadata += resp.json()["events"]
                else:
                    log.write(f"Sofascore scraper failed on {date} with status code: {resp.status_code}")
        except requests.exceptions.ConnectionError:
            log.write("Connection aborted error in src/Scrapers/sofascore.metadata()")

    return process_func(metadata)


def sofascore_scraper(url_format, name, show_progress=False):
    """
    decorator for scraping sofascore
    since you basically do the same thing for scraping different stats on sofascore
        going through each id, formatting url
    """
    def decorator(func):
        def inner(onetime=False, cur=None, log=None):
            """
            onetime: whether to scrape games already scraped
                true when data changes
            """
            scraper = cfscrape.create_scraper()
            metadata = get_sofascore_metadata(cfscraper=scraper, log=log)
            
            if onetime and cur is not None:
                cur.execute(f"""SELECT DISTINCT ON (site_id) site_id 
                FROM sofascore_{name}_data
                WHERE time_of_collection > {int((datetime.now()-timedelta(days=no_days)).timestamp())}""")
                already_scraped_ids = [x[0] for x in cur.fetchall()]
                metadata = list(filter(
                    lambda game: game["site_id"] not in already_scraped_ids,
                    metadata))

            urls = [url_format.format(_id=game["site_id"]) for game in metadata]
            len_metadata = len(metadata)

            data = []

            for game_idx, url in enumerate(urls):
                try:
                    with scraper.get(url, headers=headers) as resp:
                        if resp.status_code == 200:
                            raw = resp.json()
                            data.append(metadata[game_idx] | func(raw_data=raw) | {"time_of_collection": int(datetime.utcnow().timestamp())})
                            if show_progress:
                                log.write(f"Completed {game_idx+1} / {len_metadata} for sofascore {name}")
                                
                        elif resp.status_code == 404:
                            if show_progress:
                                log.write(f"Unable to find {game_idx+1} / {len_metadata} for sofascore {name}")
                        else:
                            log.write(f"Sofascore {name} failed on id {metadata[game_idx]['site_id']} with status code {resp.status_code}")
                except requests.exceptions.ConnectionError:
                    log.write(f"Connection aborted error in src/Scrapers/sofascore.{name}()")

            return data
        return inner
    return decorator


@sofascore_scraper(vote_url_format, "votes")
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

@sofascore_scraper(pregame_url_format, "pregame")
def pregame(*args, **kwargs):
    raw_pregame = kwargs["raw_data"]
    home_doc = raw_pregame["homeTeam"]
    away_doc = raw_pregame["awayTeam"]
    base_doc = {"home_rating": None, "home_pos": None, "home_value": None,
        "away_rating": None, "away_pos": None, "away_value": None,
        'home_res_1': None, 'home_res_2': None, 'home_res_3': None, 'home_res_4': None, 'home_res_5': None,
        'away_res_1': None, 'away_res_2': None, 'away_res_3': None, 'away_res_4': None, 'away_res_5': None}
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
                base_doc[f"{team}_res_{res_idx+1}"] = result_map[result]
    
    return base_doc


@sofascore_scraper(h2h_url_format, "h2h")
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
