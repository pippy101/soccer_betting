"""
get_all_data returns a list of all the 1x2 data from all of the bookies
All the bookie scrapers 
all scrapers are a function that yields every match
     (probs shouldn't of done that)
"""

import gevent
if __name__ == "__main__":
    from gevent import monkey
    monkey.patch_all()
from datetime import datetime
from unidecode import unidecode
from bs4 import BeautifulSoup
from pprint import pprint
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


class Scraper:
    """
    Base class for scrapers
    """
    def get_page(self, url):
        try:
            with requests.get(url, headers=headers) as resp:
                if resp.status_code == 200:
                    self.page_content[url] = resp
                else:
                    if self.url_name_dict is not None:
                        name = f"{self.site}_{self.url_name_dict[url]}"
                    else:
                        name = self.site

                    self.log.write(f"{name} encountered error code of {resp.status_code}:{url}")
                    self.page_content[url] = None

        except requests.exceptions.ConnectionError:
            self.log.write(f"Connection aborted error in {self.site}_scraper")
            
            self.page_content[url] = None


    def scrape(self):
        self.time_of_collection = datetime.utcnow().timestamp()

        self.page_content = {}
        jobs = [gevent.spawn(self.get_page, url) for url in self.urls]
        gevent.wait(jobs)
    

    def parse(self, raw_data):
        yield None

    
    def error_handling(self, error, resp):
        if self.url_name_dict is None:
            self.log.write(f"{self.site} scraper encountered error in parsing")
            filename = f"{self.site}_{round(time.time())}"
        else:
            self.log.write(f"{self.site} scraper encountered error in parsing {self.url_name_dict[resp.url]} games")
            filename = f"{self.site}_{round(time.time())}_{self.url_name_dict[resp.url]}"
        self.log.write(error)

        if 'json' in resp.headers.get('content-type'):
            with open(rf"src/Scrapers/errors/{filename}.json", "w") as fp:
                json.dump(resp.json(), fp, indent=4)
        else:
            with open(rf"src/Scrapers/errors/{filename}.html", "w") as fp:
                fp.write(BeautifulSoup(resp.content, features='lxml').prettify())

        self.log.write(f"(Raw data file saved as {filename})")


    def get_data(self, silence=False):

        if self.names is not None:
            self.url_name_dict = dict(zip(self.urls, self.names))
        else:
            self.url_name_dict = None

        self.scrape()

        cruft_fields = {
            "time_of_collection": self.time_of_collection,
            "site": self.site
        }

        matches = []
        for url in self.page_content:
            resp = self.page_content[url]
            if resp is not None:
                try:
                    for match in self.parse(resp, url=url):
                        try:
                            matches.append({**match, **cruft_fields})
                        except BaseException as e1:
                            self.log.write(e1)
                
                except BaseException as error:
                    self.error_handling(error, resp)

        return matches


class tab_scraper(Scraper):

    def __init__(self, log=None):
        self.site = "tab"
        self.urls: list
        self.urls = ["https://api.beta.tab.com.au/v1/recommendation-service/Soccer/featured?jurisdiction=NSW"]
        self.names = None
        self.log = log
    

    def parse(self, raw_data, url=None):
        raw_data = raw_data.json()
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
                                    home_odds = proposition["returnWin"]
                                    home_active = proposition["isOpen"]
                                elif proposition["position"] == "AWAY":
                                    away_odds = proposition["returnWin"]
                                    away_active = proposition["isOpen"]
                                elif proposition["position"] == "DRAW":
                                    draw_odds = proposition["returnWin"]
                                    draw_active = proposition["isOpen"]

                        break
                
                match_datetime = datetime.strptime(match["startTime"], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() + 10 * 60 * 60
                
                if None not in [home_odds, away_odds, draw_odds,
                        home_active, away_active, draw_active]:

                    yield {
                        "competition": unidecode(comp_name),
                        "site_id": str(tab_id),
                        "home_team": unidecode(home),
                        "away_team": unidecode(away),
                        "home_odds": home_odds,
                        "away_odds": away_odds,
                        "draw_odds": draw_odds,
                        "home_active": home_active,
                        "away_active": away_active,
                        "draw_active": draw_active,
                        "match_datetime": int(match_datetime)
                    }


class neds_scraper(Scraper):

    def __init__(self, log=None):
        self.price_hash = ":940b8704-e497-4a76-b390-00918ff7d282:"
        self.site = "neds"
        self.urls: list
        self.urls = ["https://api.neds.com.au/v2/sport/event-request?category_ids=%5B%2271955b54-62f6-4ac5-abaa-df88cad0aeef%22%5D&include_any_team_vs_any_team_events=true"]
        self.names = None
        self.log = log

    
    def parse(self, raw_data, url=None):
        raw_data = raw_data.json()
        for event_id in raw_data["events"]:
            event = raw_data["events"][event_id]
            if event["event_type"]["name"] != "Match":
                continue

            match_datetime = datetime.strptime(event["actual_start"], "%Y-%m-%dT%H:%M:%SZ").timestamp() + 10 * 60 * 60
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

                price_id = f"{entrant_id}{self.price_hash}"
                
                prices = raw_data["prices"][price_id]["odds"]

                try:
                    odds = prices['numerator'] / prices['denominator'] + 1
                except KeyError:
                    continue

                if outcome == "home": home_odds = odds
                elif outcome == "away": away_odds = odds
                elif outcome == "draw": draw_odds = odds

            if None not in [home_odds, away_odds, draw_odds]:
                yield {
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
                    "match_datetime": int(match_datetime)
                }

class ps3838_scraper(Scraper):
    
    def __init__(self, log=None):
        self.site = "ps3838"
        self.urls: list
        self.urls = [
            # upcoming and live
            "https://www.ps3838.com/sports-service/sv/compact/events?_g=1&btg=1&c=&cl=3&d=&ev=&g=&hle=true&l=3&lg=&lv=&me=0&mk=1&more=false&o=1&ot=1&pa=0&pn=-1&sp=29&tm=0&v=0&wm=&locale=en_US&_=1657327044177&withCredentials=true",
            # today
            "https://www.ps3838.com/sports-service/sv/compact/events?_g=1&btg=1&c=&cl=3&d=&ev=&g=&hle=true&l=3&lg=&lv=&me=0&mk=1&o=1&ot=1&pa=0&pn=-1&sp=29&tm=0&v=0&wm=&locale=en_US&_={time}&withCredentials=true"
        ]
        self.urls = [
            "https://www.ps3838.com/sports-service/sv/compact/events?",
            "https://www.ps3838.com/sports-service/sv/compact/events?_g=1&btg=1&c=&cl=3&d=&ev=&g=&hle=true&l=3&lg=&lv=&me=0&mk=0&more=false&o=1&ot=1&pa=0&pn=-1&sp=29&tm=0&v=0&wm=&locale=en_US&_=1658631397895&withCredentials=true"
            ]
        self.names = None
        self._ids = []
        self.log = log
    
    def get_page(self, url):
        url = url.format(time=round(time.time()))
        try:
            with requests.get(url, headers=headers) as resp:
                if resp.status_code == 200:
                    self.page_content[url] = resp
                else:
                    if self.url_name_dict is not None:
                        name = f"{self.site}_{self.url_name_dict[url]}"
                    else:
                        name = self.site

                    self.log.write(f"{name} encountered error code of {resp.status_code}: {url}")
                    self.page_content[url] = None

        except requests.exceptions.ConnectionError:
            self.log.write(f"Connection aborted error in {self.site}_scraper")
            self.page_content[url] = None

    def parse(self, raw_data, url=None):
        raw_data = raw_data.json()
        poss_games = ["hle", "n", "l"]
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

                odds_list = match_data[8]["0"][-6]
                
                if odds_list is None: continue
                
                ps3838_id = match_data[0]
                if ps3838_id in self._ids:
                    continue
                self._ids.append(ps3838_id)
                home = match_data[1]
                away = match_data[2]
                match_datetime = match_data[4] / 1000

                home_odds = away_odds = draw_odds = None
                home_active = away_active = draw_active = None

                if odds_list[0] != "":
                    away_odds, home_odds, draw_odds = map(float, odds_list[:3])
                    home_active = away_active = draw_active = True
                else:
                    home_active = away_active = draw_active = False
                
                yield {
                    "competition": unidecode(f"{country} {comp}"),
                    "site_id": str(ps3838_id),
                    "home_team": unidecode(home),
                    "away_team": unidecode(away),
                    "home_odds": home_odds,
                    "away_odds": away_odds,
                    "draw_odds": draw_odds,
                    "home_active": home_active,
                    "away_active": away_active,
                    "draw_active": draw_active,
                    "match_datetime": int(match_datetime)
                }


class palmerbet_scraper(Scraper):
    
    def __init__(self, log=None):
        self.site = "palmerbet"
        self.urls: list
        self.urls = ["https://fixture.palmerbet.online/fixtures/sports/b4073512-cdd5-4953-950f-3f7ad31fa955/matches?sportType=soccer&pageSize=1000&channel=website"]
        self.names = None
        self.log = log

    def parse(self, raw_data, url=None):
        raw_data = raw_data.json()
        for match in raw_data["matches"]:

            home_active = away_active = draw_active = False
            home_odds = away_odds = draw_odds = None

            palmerbet_id = match["eventId"]

            home = match["homeTeam"]["title"]
            away = match["awayTeam"]["title"]
            
            if "draw" in match:
                if "win" in match["homeTeam"] and "price" in match["homeTeam"]["win"]:
                    home_odds = match["homeTeam"]["win"]["price"]
                if "win" in match["awayTeam"] and "price" in match["awayTeam"]["win"]:
                    away_odds = match["awayTeam"]["win"]["price"]
                if "price" in match["draw"]:
                    draw_odds = match["draw"]["price"]
            
            comp = match["paths"][2]["title"]

            match_datetime = datetime.strptime(match["startTime"], "%Y-%m-%dT%H:%M:%SZ").timestamp() + 10 * 60 * 60

            yield {
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
                "match_datetime": int(match_datetime)
            }


class onex_scraper(Scraper):
    
    def __init__(self, log=None):
        self.site = "onex"
        self.urls: list
        self.urls = [
            # in play
            "https://1xbet.com/LiveFeed/Get1x2_VZip?sports=1&count=50&lng=en&mode=4&country=169&getEmpty=true&noFilterBlockEvent=true",
            # upcoming
            "https://1xbet.com/LineFeed/Get1x2_VZip?sports=1&count=50&lng=en&tf=2200000&tz=3&mode=4&country=169&getEmpty=true"
        ]
        self.names = ["in-play", "upcoming"]
        self.log = log
    

    def parse(self, raw_data, url):
        raw_data = raw_data.json()
        for match in raw_data["Value"]:
            oneX_id = match["I"]
            home = match['O1']
            away = match["O2"]
            # country = match["CN"]
            comp = match["L"]
            match_datetime = match["S"]
            home_odds = away_odds = draw_odds = None
            home_active = away_active = draw_active = None
            for prop in match["E"]:
                p_type = prop["T"]
                if p_type == 1: home_odds = prop["C"]
                if p_type == 2: draw_odds = prop["C"]
                if p_type == 3: away_odds = prop["C"]
                
                if None not in [home_odds, draw_odds, away_odds]:
                    break
            
            yield {
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
                "match_datetime": int(match_datetime)
            }


class interwetten_scraper(Scraper):

    def __init__(self, log=None):
        self.site = "interwetten"
        self.urls: list
        self.urls = ["https://www.interwetten.com/en/sport/upcoming?hours=48"]
        self.names = None
        self.log = log
    
    def parse(self, raw_data, url=None):
        raw_data = BeautifulSoup(raw_data.content, features='lxml')

        league = None
        for tr in raw_data.find("table", {"class": "bets"}).findAll("tr"):
            tr_attrs = tr.attrs
            if tr_attrs is None or "class" not in tr_attrs:
                td = tr.find("td", {"class": "fvd"})
                if td is None:
                    continue
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
                match_datetime = bet_data[-1] + 12 * 60 * 60

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
                
                yield {
                    "competition": unidecode(comp),
                    "site_id": str(interwetten_id),
                    "home_team": unidecode(home),
                    "away_team": unidecode(away),
                    "home_odds": home_odds,
                    "away_odds": away_odds,
                    "draw_odds": draw_odds,
                    "match_datetime": int(match_datetime)
                }


class bluebet_scraper(Scraper):

    def __init__(self, log=None):
        self.site = "bluebet"
        self.urls: list
        self.urls = ["https://web20-api.bluebet.com.au/MasterCategory?EventTypeId=100&WithLevelledMarkets=true"]
        self.names = None
        self.log = log

    def parse(self, raw_data, url=None):
        raw_data = raw_data.json()
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
                    match_datetime = datetime.strptime(
                        game["MaxAdvertisedStartTime"][:-9],
                        "%Y-%m-%dT%H:%M:%S"
                    ).timestamp() + 10 * 60 * 60
                    home_odds = away_odds = draw_odds = None
                    for market in game["Markets"]:
                        name = market["OutcomeName"]
                        if name == home: home_odds = market["Price"]
                        elif name == away: away_odds = market["Price"]
                        elif name == "Draw": draw_odds = market["Price"]
                        
                        if None not in [home_odds, away_odds, draw_odds]:
                            break
                    
                    yield {
                        "competition": unidecode(f"{country} {comp}"),
                        "site_id": str(bluebet_id),
                        "home_team": unidecode(home),
                        "away_team": unidecode(away),
                        "home_odds": home_odds,
                        "away_odds": away_odds,
                        "draw_odds": draw_odds,
                        "match_datetime": int(match_datetime)
                    }


class playup_scraper(Scraper):

    def __init__(self, log=None):
        self.site = "playup"
        self.urls: list
        self.urls = ["https://wagering-api.playup.io/v1/sport_events/?include=primary_market_group.markets.selections,participants,sportcast_fixture&filter[status_id]=1,7&page[size]=200"]
        self.names = None
        self.i = 1
        self.log = log

    def scrape(self):
        self.time_of_collection = datetime.utcnow().timestamp()
   
        self.page_content = {}
        self.get_page(self.urls[0])
        no_pages = self.page_content[self.urls[0]].json()["meta"]["pagination"]["total_pages"]
        new_url_format = "https://wagering-api.playup.io/v1/sport_events?include=primary_market_group.markets.selections,participants,sportcast_fixture&filter%5Bstatus_id%5D=1%2C7&page%5Bnumber%5D={page_no}&page%5Bsize%5D=200"
        new_urls = list([new_url_format.format(page_no=page_no + 1) for page_no in range(1, no_pages)])
        jobs = [gevent.spawn(self.get_page, new_url) for new_url in new_urls]
        gevent.wait(jobs)
    

    def parse(self, raw_data, url=None):
        raw_data = raw_data.json()
        if "included" not in raw_data:
            return []

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
                match_datetime = datetime.strptime(event["attributes"]["start_time"][:-6], "%Y-%m-%dT%H:%M:%S").timestamp() + 10 * 60 * 60
                comp = event["attributes"]["competition"]["name"]
                if event["relationships"]["primary_market_group"]["data"] is None:
                    continue
                market_group_id = event["relationships"]["primary_market_group"]["data"]["id"]
                games[playup_id] = {
                    "home_team": unidecode(home),
                    "away_team": unidecode(away),
                    "match_datetime": int(match_datetime),
                    "competition": comp,
                    "site_id": str(playup_id),
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
                
                yield_game = {**game, **odds_dict}
                del yield_game["_market_group_id"]
                yield yield_game
    

def _gevent_scraper_data(_scraper, dict_loc):
    dict_loc[_scraper.site] = _scraper.get_data(silence=True)


def _get_all_data(log=sys.stdout):
    # returns dict grouped by site
    scrapers = (
        neds_scraper(log=log),
        tab_scraper(log=log),
        ps3838_scraper(log=log),
        palmerbet_scraper(log=log),
        onex_scraper(log=log),
        interwetten_scraper(log=log),
        bluebet_scraper(log=log),
        playup_scraper(log=log),
    )
    data = {}
    jobs = [gevent.spawn(_gevent_scraper_data, _scraper, data) for _scraper in scrapers]
    gevent.wait(jobs)
    
    return data

def scraper_data(_scraper, list_loc):
    list_loc += _scraper.get_data()

# returns 1-d list of games
def get_all_data(log=sys.stdout):
    scrapers = (
        neds_scraper(log=log),
        tab_scraper(log=log),
        ps3838_scraper(log=log),
        palmerbet_scraper(log=log),
        onex_scraper(log=log),
        interwetten_scraper(log=log),
        bluebet_scraper(log=log),
        playup_scraper(log=log),
    )
    data = []
    jobs = [gevent.spawn(scraper_data, _scraper, data) for _scraper in scrapers]
    gevent.wait(jobs)

    return data
