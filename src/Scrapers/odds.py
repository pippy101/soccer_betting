"""
get_all_data returns a list of all the 1x2 data from all of the bookies
All the bookie scrapers 
all scrapers are a function that yields every match
     (probs shouldn't of done that)
almost definitely too many lines in this 
"""

import gevent
if __name__ == "__main__":
    from gevent import monkey
    monkey.patch_all()
from datetime import datetime
from unidecode import unidecode
from bs4 import BeautifulSoup
from math import ceil
import time
import requests
import json
import re
import sys

headers = {
    "accept-encoding": "gzip, deflate, br",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36",
    "content-type": "application/json"
}


def get_page(url, log, name):
    with requests.get(url, headers=headers) as resp:
        if resp.status_code == 200:
            return resp
        else:
            log.write(f"{name} encountered error code of {resp.status_code}:{url}\n")
            return


def add_get_page(url, log, name, list_loc):
    list_loc.append(get_page(url, log, name))


def tab(log=sys.stdout):
    site = "tab"
    url = "https://api.beta.tab.com.au/v1/recommendation-service/Soccer/featured?jurisdiction=NSW"

    resp = get_page(url, log, site)
    if resp is None: return {}
    else: raw_data = resp.json()

    cruft_fields = {
        "time_of_collection": datetime.utcnow(),
        "site": site
    }

    game_data = []
    for competition in raw_data["competitions"]:
        comp_name = competition["name"]
        for match in competition["matches"]:
            tab_id = match["spectrumUniqueId"]
            home = away = None

            for team in match["contestants"]:
                if team["isHome"]:
                    home = team["name"]
                else:
                    away = team["name"]

            if home is None or away is None:
                continue
            
            home_odds = away_odds = draw_odds = None
            home_active = away_active = draw_active = None
            for market in match["markets"]:
                if market["betOption"] == "Result":
                    for proposition in market["propositions"]:
                        if "position" in proposition:
                            if proposition["position"] == "HOME":
                                home_odds = float(proposition["returnWin"])
                                home_active = proposition["isOpen"]
                            elif proposition["position"] == "AWAY":
                                away_odds = float(proposition["returnWin"])
                                away_active = proposition["isOpen"]
                            elif proposition["position"] == "DRAW":
                                draw_odds = float(proposition["returnWin"])
                                draw_active = proposition["isOpen"]

                    break
            
            game_time = datetime.strptime(match["startTime"], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() + 10 * 60 * 60
            
            if None not in [home_odds, away_odds, draw_odds,
                    home_active, away_active, draw_active]:

                game_data.append({
                    "competition": unidecode(comp_name),
                    "site_id": int(tab_id),
                    "home_team": unidecode(home),
                    "away_team": unidecode(away),
                    "home_odds": home_odds,
                    "away_odds": away_odds,
                    "draw_odds": draw_odds,
                    "home_active": home_active,
                    "away_active": away_active,
                    "draw_active": draw_active,
                    "game_time": datetime.fromtimestamp(game_time),
                    **cruft_fields
                })

    return game_data
tab.data_type = "match"
tab.site = "tab"


def neds(log=sys.stdout):
    price_hash = ":940b8704-e497-4a76-b390-00918ff7d282:"
    site = "neds"
    url = "https://api.neds.com.au/v2/sport/event-request?category_ids=%5B%2271955b54-62f6-4ac5-abaa-df88cad0aeef%22%5D&include_any_team_vs_any_team_events=true"

    resp = get_page(url, log, site)
    if resp is None: return {}
    else: raw_data = resp.json()

    cruft_fields = {
        "time_of_collection": datetime.utcnow(),
        "site": site
    }

    game_data = []
    for event_id in raw_data["events"]:
            event = raw_data["events"][event_id]
            if event["event_type"]["name"] != "Match":
                continue

            game_time = datetime.strptime(event["actual_start"], "%Y-%m-%dT%H:%M:%SZ").timestamp() + 10 * 60 * 60
            try:
                home, away = event["name"].split(" vs ")
            except ValueError:
                continue
            home_active = away_active = draw_active = event["match_status"] == "BettingOpen"

            main_markets = event["main_markets"]
            market_id = main_markets[0]
            entrant_ids = raw_data["markets"][market_id]["entrant_ids"]
            
            if "competition" not in event:
                continue
            
            # country = event["competition"]["region"]
            comp = event["competition"]["name"]
            
            home_odds = away_odds = draw_odds = None
            for entrant_id in entrant_ids:
                entrant = raw_data["entrants"][entrant_id]
                outcome = entrant["name"]
                if outcome != "Draw":
                    if "home_away" not in entrant:
                        continue
                    outcome = entrant["home_away"]
                outcome = outcome.lower()

                price_id = f"{entrant_id}{price_hash}"
                
                prices = raw_data["prices"][price_id]["odds"]

                if 'numerator' in prices and "denominator" in prices:
                    odds = prices['numerator'] / prices['denominator'] + 1

                if outcome == "home": home_odds = float(odds)
                elif outcome == "away": away_odds = float(odds)
                elif outcome == "draw": draw_odds = float(odds)

            game_data.append({
                "competition": unidecode(comp),
                "site_id": str(event_id),
                "home_team": unidecode(home),
                "away_team": unidecode(away),
                "home_odds": home_odds,
                "away_odds": away_odds,
                "draw_odds": draw_odds,
                "home_active": home_active,
                "away_active": away_active,
                "draw_active": draw_active,
                "game_time": datetime.fromtimestamp(game_time),
                **cruft_fields
            })
        
    return game_data
neds.data_type = "match"
neds.site = "neds"


def ps3838(log=sys.stdout):
    site = "ps3838"
    urls = [
            "https://www.ps3838.com/sports-service/sv/compact/events?",
            "https://www.ps3838.com/sports-service/sv/compact/events?_g=1&btg=1&c=&cl=3&d=&ev=&g=&hle=true&l=3&lg=&lv=&me=0&mk=0&more=false&o=1&ot=1&pa=0&pn=-1&sp=29&tm=0&v=0&wm=&locale=en_US&_=1658631397895&withCredentials=true"
            ]
    _ids = []
    poss_games = ["hle", "n", "l"]

    matches = []

    cruft_fields = {
        "time_of_collection": datetime.utcnow(),
        "site": site
    }

    responses = [get_page(url, log, site) for url in urls]
    for resp in responses:
        if resp is None: continue
        raw_data = resp.json()

        raw_data_games = []
        for part in poss_games:
            if raw_data[part] is not None and raw_data[part] != []:
                raw_data_games += raw_data[part][0][2]
                
        for league_data in raw_data_games:
            name = league_data[1]

            if " - " in name:
                country, comp = name.split(" - ")
            else:
                country = name
                comp = None

            for match_data in league_data[2]:
                if "0" not in match_data[8]: continue

                odds_list = match_data[8]["0"][-7]
                if type(odds_list) == int: continue
                
                if odds_list is None: continue
                
                ps3838_id = match_data[0]
                if ps3838_id in _ids:
                    continue
                _ids.append(ps3838_id)
                home = match_data[1]
                away = match_data[2]
                game_time = match_data[4] / 1000

                home_odds = away_odds = draw_odds = None
                home_active = away_active = draw_active = None

                if odds_list[0] != "":
                    away_odds, home_odds, draw_odds = map(float, odds_list[:3])
                    home_active = away_active = draw_active = True
                else:
                    home_active = away_active = draw_active = False
                
                matches.append({
                    "competition": unidecode(f"{country} {comp}"),
                    "site_id": int(ps3838_id),
                    "home_team": unidecode(home),
                    "away_team": unidecode(away),
                    "home_odds": home_odds,
                    "away_odds": away_odds,
                    "draw_odds": draw_odds,
                    "home_active": home_active,
                    "away_active": away_active,
                    "draw_active": draw_active,
                    "game_time": datetime.fromtimestamp(game_time),
                    **cruft_fields
                })
                
    return matches
ps3838.data_type = "match"
ps3838.site = "ps3838"


def palmerbet(log=sys.stdout):
    site = "palmerbet"
    url = "https://fixture.palmerbet.online/fixtures/sports/b4073512-cdd5-4953-950f-3f7ad31fa955/matches?sportType=soccer&pageSize=1000&channel=website"
    
    cruft_fields = {
        "time_of_collection": datetime.utcnow(),
        "site": site
    }

    resp = get_page(url, log, site)
    if resp is None: return {}
    raw_data = resp.json()

    games = []
    for match in raw_data["matches"]:

        home_active = away_active = draw_active = False
        home_odds = away_odds = draw_odds = None

        palmerbet_id = match["eventId"]

        home = match["homeTeam"]["title"]
        away = match["awayTeam"]["title"]
        
        if "draw" in match:
            if "win" in match["homeTeam"] and "price" in match["homeTeam"]["win"]:
                home_odds = float(match["homeTeam"]["win"]["price"])
            if "win" in match["awayTeam"] and "price" in match["awayTeam"]["win"]:
                away_odds = float(match["awayTeam"]["win"]["price"])
            if "price" in match["draw"]:
                draw_odds = float(match["draw"]["price"])
        
        comp = match["paths"][2]["title"]

        game_time = datetime.strptime(match["startTime"], "%Y-%m-%dT%H:%M:%SZ").timestamp() + 10 * 60 * 60

        games.append({
            "competition": unidecode(comp),
            "site_id": str(palmerbet_id),
            "home_team": unidecode(home),
            "away_team": unidecode(away),
            "home_odds": home_odds,
            "away_odds": away_odds,
            "draw_odds": draw_odds,
            "home_active": home_active,
            "away_active": away_active,
            "draw_active": draw_active,
            "game_time": datetime.fromtimestamp(game_time),
            **cruft_fields
        })
    
    return games
palmerbet.data_type = "match"
palmerbet.site = "palmerbet"


def onex(log=sys.stdout):
    site = "palmerbet"
    urls = [
            # in play
            "https://1xbet.com/LiveFeed/Get1x2_VZip?sports=1&count=1000&lng=en&mode=4&country=169&getEmpty=true&noFilterBlockEvent=true",
            # upcoming
            "https://1xbet.com/LineFeed/Get1x2_VZip?sports=1&count=1000&lng=en&tf=2200000&tz=3&mode=4&country=169&getEmpty=true"
        ]

    cruft_fields = {
        "time_of_collection": datetime.utcnow(),
        "site": site
    }
    
    for url in urls:
        resp = get_page(url, log, site)
        if resp is None: return {}
        raw_data = resp.json()

        games = []
        for match in raw_data["Value"]:
            try:
                oneX_id = match["I"]
                home = match['O1']
                away = match["O2"]
                comp = match["L"]
                game_time = match["S"]
                home_odds = away_odds = draw_odds = None
                home_active = away_active = draw_active = None
                for prop in match["E"]:
                    p_type = prop["T"]
                    if p_type == 1: home_odds = float(prop["C"])
                    if p_type == 2: draw_odds = float(prop["C"])
                    if p_type == 3: away_odds = float(prop["C"])
                    
                    if None not in [home_odds, draw_odds, away_odds]:
                        break
                    
            except KeyError:
                continue
            
            games.append({
                "competition": unidecode(comp),
                "site_id": str(oneX_id),
                "home_team": unidecode(home),
                "away_team": unidecode(away),
                "home_odds": home_odds,
                "away_odds": away_odds,
                "draw_odds": draw_odds,
                "home_active": home_active,
                "away_active": away_active,
                "draw_active": draw_active,
                "game_time": datetime.fromtimestamp(game_time),
                **cruft_fields
            })
    
    return games
onex.data_type = "match"
onex.site = "onex"


def interwetten(log=sys.stdout):
    site = "interwetten"
    url = "https://www.interwetten.com/en/sport/upcoming?hours=48"

    cruft_fields = {
        "time_of_collection": datetime.utcnow(),
        "site": site
    }

    resp = get_page(url, log, site)
    if resp is None: return {}
    raw_data = BeautifulSoup(resp.content, features='lxml')

    games = []
    for tr in raw_data.find("table", {"class": "bets"}).findAll("tr"):
        tr_attrs = tr.attrs
        if tr_attrs is None or "class" not in tr_attrs:
            td = tr.find("td", {"class": "fvd"})
            if td is None: continue

            comp = td.get_text()
            comp = re.sub(r"(^[\r\n ]+)|([ \n\r]+$)| / Match|TOP Club", "", comp)
            comp = comp.replace(" / Match", "")
        elif tr_attrs is not None and \
                ("even" in tr_attrs["class"] or "odd" in tr_attrs["class"])\
                    and "n3x" not in tr_attrs["class"]:

            bet_tr = tr.find("tr", {"class": "n3x"})
            if bet_tr is None:
                continue

            bet_data = json.loads(bet_tr["data-betting"])
            interwetten_id = bet_data[0]
            game_time = bet_data[-1] + 12 * 60 * 60

            home_odds = away_odds = draw_odds = None
            home = away = None
            
            for prop_td in bet_tr.findAll("td", {"class": "BETTINGOFFER"}):
                prop_data = json.loads(prop_td["data-betting"])
                prop_type = prop_data[1]
                if prop_type == "1": 
                    home_odds = float(prop_data[-2].replace(",", "."))
                    home = prop_data[2]
                if prop_type == "2": 
                    away_odds = float(prop_data[-2].replace(",", "."))
                    away = prop_data[2]
                if prop_type == "X": 
                    draw_odds = float(prop_data[-2].replace(",", "."))
                
                if None not in [home_odds, away_odds, draw_odds]:
                    break
            
            games.append({
                "competition": unidecode(comp),
                "site_id": int(interwetten_id),
                "home_team": unidecode(home),
                "away_team": unidecode(away),
                "home_odds": home_odds,
                "away_odds": away_odds,
                "draw_odds": draw_odds,
                "game_time": datetime.fromtimestamp(game_time),
                **cruft_fields
            })
    return games
interwetten.data_type = "match"
interwetten.site = "interwetten"


def bluebet(log=sys.stdout):
    site = "bluebet"
    url = "https://web20-api.bluebet.com.au/MasterCategory?EventTypeId=100&WithLevelledMarkets=true"

    cruft_fields = {
        "time_of_collection": datetime.utcnow(),
        "site": site
    }

    resp = get_page(url, log, site)
    if resp is None: return {}
    raw_data = resp.json()

    games = []
    for country_cat in raw_data["MasterCategories"]:
        country = country_cat["MasterCategory"]
        for comp_cat in country_cat["Categories"]:
            comp = comp_cat["CategoryName"]
            for game in comp_cat["MasterEvents"]:
                event_name = game["MasterEventName"]
                if " v " not in event_name or "Markets" not in game:
                    continue
                bluebet_id = game["MasterEventId"]
                home, away = event_name.split(" v ")
                game_time = datetime.strptime(
                    game["MaxAdvertisedStartTime"][:-9],
                    "%Y-%m-%dT%H:%M:%S"
                ).timestamp() + 10 * 60 * 60
                home_odds = away_odds = draw_odds = None
                for market in game["Markets"]:
                    name = market["OutcomeName"]
                    if name == home: home_odds = float(market["Price"])
                    elif name == away: away_odds = float(market["Price"])
                    elif name == "Draw": draw_odds = float(market["Price"])
                    
                    if None not in [home_odds, away_odds, draw_odds]:
                        break
                
                games.append({
                    "competition": unidecode(f"{country} {comp}"),
                    "site_id": bluebet_id,
                    "home_team": unidecode(home),
                    "away_team": unidecode(away),
                    "home_odds": home_odds,
                    "away_odds": away_odds,
                    "draw_odds": draw_odds,
                    "game_time": datetime.fromtimestamp(game_time),
                    **cruft_fields
                })
    
    return games
bluebet.data_type = "match"
bluebet.site = "bluebet"


def playup(log=sys.stdout):
    site = "playup"
    first_url = "https://wagering-api.playup.io/v1/sport_events/?include=primary_market_group.markets.selections,participants,sportcast_fixture&filter[status_id]=1,7&page[size]=200"
    paginated_url = "https://wagering-api.playup.io/v1/sport_events?include=primary_market_group.markets.selections,participants,sportcast_fixture&filter%5Bstatus_id%5D=1%2C7&page%5Bnumber%5D={page_no}&page%5Bsize%5D=200"

    cruft_fields = {
        "time_of_collection": datetime.utcnow(),
        "site": site
    }

    base_page = get_page(first_url, log, site)
    if base_page is None: return {}
    no_pages = base_page.json()["meta"]["pagination"]["total_pages"]

    responses = []
    new_urls = list([paginated_url.format(page_no=page_no + 1) for page_no in range(1, no_pages)])
    jobs = [gevent.spawn(add_get_page, new_url, log, site, responses) for new_url in new_urls]
    gevent.wait(jobs)

    for resp in responses:
        if resp is None: continue
        raw_data = resp.json()
        if "included" not in raw_data: continue

        all_games = []

        games = {}  # indexed by playup id
        primary_market_groups = {}
        markets = {}
        selections = {}
        for event in raw_data["data"]:
            if event["type"] == "sport_events" \
                    and event["attributes"]["sport"]["name"] == "Soccer"\
                        and "relationships" in event\
                            and " v " in event["attributes"]["name"]:
                playup_id = event["id"]
                home, away = event["attributes"]["name"].split(" v ")
                game_time = datetime.strptime(event["attributes"]["start_time"][:-6], "%Y-%m-%dT%H:%M:%S").timestamp() + 10 * 60 * 60
                comp = event["attributes"]["competition"]["name"]
                if event["relationships"]["primary_market_group"]["data"] is None:
                    continue
                market_group_id = event["relationships"]["primary_market_group"]["data"]["id"]
                games[playup_id] = {
                    "home_team": unidecode(home),
                    "away_team": unidecode(away),
                    "game_time": datetime.fromtimestamp(game_time),
                    "competition": comp,
                    "site_id": int(playup_id),
                    "_market_group_id": market_group_id
                }
        
        for event in raw_data["included"]:
            
            if event["type"] == "event_market_groups" and event["attributes"]["name"] == "Primary Markets":
                market_ids = []
                for market in event["relationships"]["markets"]["data"]:
                    market_ids.append(market["id"])

                primary_market_groups[event["id"]] = market_ids
            
            elif event["type"] == "markets" and event["attributes"]["line"] is None:
                selection_ids = []
                for selection in event["relationships"]["selections"]["data"]:
                    selection_ids.append(selection["id"])
                
                markets[event["id"]] = selection_ids
            
            elif event["type"] == "selections" and event["attributes"]["type"] == "Sport":
                selections[event["id"]] = event
        
        for game_id in games:
            game = games[game_id]
            if game["_market_group_id"] not in primary_market_groups:
                continue
            for market_id in primary_market_groups[game["_market_group_id"]]:
                if market_id not in markets:
                    continue

                odds_dict = {
                    "home_odds": None, "away_odds": None, "draw_odds": None,
                    "home_active": None, "away_active": None, "draw_active": None
                }
                
                for selection_id in markets[market_id]:
                    attributes = selections[selection_id]["attributes"]
                    name = attributes["name"]
                    if name == game["home_team"]: 
                        odds_dict["home_odds"] = attributes["price"] / 100
                        odds_dict["home_active"] = attributes["status"]["name"] == "Active"
                    if name == game["away_team"]: 
                        odds_dict["away_odds"] = attributes["price"] / 100
                        odds_dict["away_active"] = attributes["status"]["name"] == "Active"
                    if name == "Draw": 
                        odds_dict["draw_odds"] = attributes["price"] / 100
                        odds_dict["draw_active"] = attributes["status"]["name"] == "Active"

                    if None not in odds_dict.values():
                        break
                
                yield_game = {**game, **odds_dict, **cruft_fields}
                del yield_game["_market_group_id"]
                all_games.append(yield_game)
    return all_games
playup.data_type = "match"
playup.site = "playup"


with open(r"src/Scrapers/bf_post.json", "r") as fp:
    betfair_request_payload = json.load(fp)


def betfair(log=sys.stdout):
    site = "betfair"
    search_url = "https://scan-inbf.betfair.com.au/www/sports/navigation/facet/v1/search?_ak=nzIFcwyWhrlwYMrh&alt=json"
    market_url = "https://ero.betfair.com.au/www/sports/exchange/readonly/v1/bymarket?_ak=nzIFcwyWhrlwYMrh&alt=json&currencyCode=AUD&locale=en&marketIds={market_ids}&rollupLimit=25&rollupModel=STAKE&types=MARKET_STATE,MARKET_RATES,MARKET_DESCRIPTION,EVENT,RUNNER_DESCRIPTION,RUNNER_STATE,RUNNER_EXCHANGE_PRICES_BEST,RUNNER_METADATA,MARKET_LICENCE,MARKET_LINE_RANGE_INFO"

    with requests.post(search_url, headers=headers, json=betfair_request_payload) as resp:
        if resp.status_code == 200:
            search_results = resp.json()
        else:
            log.write(f"{site} encountered error code of {resp.status_code}: {search_url}\n")
            return {}

    # with open(r"test/betfair/betfair_search.json", "w") as fp:
    #     json.dump(search_results, fp, indent=4)

    market_ids = [event["marketId"] for event in search_results["results"]]
    competition_ids = search_results["attachments"]["competitions"]
    # event_id -> competition name
    competition_map = {event["eventId"]: competition_ids[str(event["competitionId"])]["name"] for event in search_results["results"]}

    cruft_fields = {
        "time_of_collection": datetime.utcnow(),
        "site": site
    }
    # betfair only returns a max of 25 games per request
    get_url = lambda ids: market_url.format(market_ids=','.join(ids))
    responses = []
    jobs = [gevent.spawn(add_get_page, get_url(market_ids[i*25:(i+1)*25]), log, site, responses) for i in range(int(ceil(len(market_ids) / 25)))]
    gevent.wait(jobs)
    # i = 0
    games = []
    for resp in responses:
        if resp is None: continue
        page = resp.json()
        for event in page["eventTypes"][0]["eventNodes"]:
            market_1x2 = list(filter(
                lambda x: x["description"]["marketName"].lower() == "match odds",
                event["marketNodes"]
            ))
            if len(market_1x2) == 0: continue

            market_1x2 = market_1x2[0]
            event_name = event['event']["eventName"]
            home_team, away_team = event_name.split(" v ")
            game_time = int(datetime.strptime(event["event"]["openDate"], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())
            competition = competition_map[event["eventId"]]

            odds_data = {"home_odds_back": [], "home_volume_back": [], "home_odds_lay": [], "home_volume_lay": [],
            "away_odds_back": [], "away_volume_back": [], "away_odds_lay": [], "away_volume_lay": [],
            "draw_odds_back": [], "draw_volume_back": [], "draw_odds_lay": [], "draw_volume_lay": []}

            for runner in market_1x2["runners"]:
                runner_name = runner["description"]["runnerName"]
                if runner_name == home_team: runner_type = "home"
                if runner_name == away_team: runner_type = "away"
                if runner_name == "The Draw": runner_type = "draw"
                
                if "availableToBack" in runner["exchange"]:
                    for market in runner["exchange"]["availableToBack"]:
                        odds_data[f"{runner_type}_odds_back"].append(market["price"])
                        odds_data[f"{runner_type}_volume_back"].append(market["size"])
                if "availableToLay" in runner["exchange"]:
                    for market in runner["exchange"]["availableToLay"]:
                        odds_data[f"{runner_type}_odds_lay"].append(market["price"])
                        odds_data[f"{runner_type}_volume_lay"].append(market["size"])
            
            games.append({
                "home_team": home_team,
                "away_team": away_team,
                "game_time": datetime.fromtimestamp(game_time),
                "competition": competition,
                "site_id": event["eventId"],
                "total_matched": market_1x2["state"]["totalMatched"],
                "total_available": market_1x2["state"]["totalAvailable"],
                **odds_data,
                **cruft_fields
            })

        # i += 1
        # with open(rf"test/betfair/betfair_page_{i}.json", "w") as fp:
        #     json.dump(page, fp, indent=4)

    # with open(rf"test/betfair/betfair_proc.json", "w") as fp:
    #     json.dump(games, fp, indent=4)

    return games
betfair.data_type = "match"
betfair.site = "betfair"


def scraper_data(_scraper, list_loc, log):
    list_loc += _scraper(log=log)


# returns 1-d list of games
def get_all_data(log=sys.stdout):
    scrapers = (
        neds,
        tab,
        ps3838,
        palmerbet,
        onex,
        interwetten,
        bluebet,
        playup,
        betfair
    )
    data = []
    jobs = [gevent.spawn(scraper_data, _scraper=_scraper, list_loc=data, log=log) for _scraper in scrapers]
    gevent.wait(jobs)

    return data
