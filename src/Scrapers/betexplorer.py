"""
outcomes give list of scores (for both live and finished games)
returns None for home and away score, in the case of postponed or similar
"""

import gevent
if __name__ == "__main__":
    from gevent import monkey; monkey.patch_all()
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from unidecode import unidecode
import requests
import re
import sys

headers = {
    "accept-encoding": "gzip, deflate, br",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36",
    "content-type": "application/json"
}
cookies = {"my_timezone": "+0"}
no_days = 3  # number of days in the past to scrape outcome data from
outcome_url_format = "https://www.betexplorer.com/results/soccer/?year=%Y&month=%m&day=%d"

def get_resp(url, log, headers=headers, cookies=cookies):
    try:
        with requests.get(url, headers=headers, cookies=cookies) as resp:
            if resp.status_code == 200:
                return resp
            else:
                log.write(f"Betexplorer outcome scraper encountered status code {resp.status_code}")
                return
    except requests.exceptions.ConnectionError:
        log.write("Connection aborted error")
        return


def get_metadata(date, metadata, site_ids, log=sys.stdout):
    url = date.strftime(outcome_url_format)
    time_of_collection = datetime.utcnow()
    page_response = get_resp(url, log)
    if page_response is None: return
    page_content = page_response.content
    
    live_resp = get_resp("https://www.betexplorer.com/gres/ajax/live-results.php", log, headers={"referer": "https://www.betexplorer.com/next/soccer/", **headers})
    if live_resp is None: return
    live_scores = live_resp.json()
    
    page_soup = BeautifulSoup(page_content, features="lxml")
    outcome_table = page_soup.find("div", {"id": "nr-all"}).find("table", {"class": "table-main"})
    comp = None
    for tr in outcome_table.findAll("tr"):
        attrs = tr.attrs
        if "class" in attrs:
            comp = tr.find("a").get_text().replace(":", "")
        
        else:
            a = tr.find("a")
            teams = a.get_text()
            if " - " in teams:
                teams = teams.split(" - ")
                if len(teams) == 2:
                    home, away = teams
                else:
                    continue
            else:
                continue

            explorer_id = a["href"].split("/")[-2]
            if explorer_id in site_ids:
                continue

            match_date_string = attrs["data-dt"]
            game_time = datetime.strptime(match_date_string, "%d,%m,%Y,%H,%M").timestamp()
            
            game_completed = True
            game_stage = None
            if live_scores is not None and explorer_id in live_scores["events"]\
                    and live_scores["events"][explorer_id]["finished"] == 0:
                game_completed = False
                score_text = live_scores["events"][explorer_id]["score"]
                home_score, away_score = map(int, score_text.split(":"))
                
                game_stage_code = live_scores["events"][explorer_id]["event_stage_id"]
                game_stage = live_scores["event_stages"][str(game_stage_code)][1]

            else:
                score_elem = tr.find("td", {"class": "table-main__result"})
                if score_elem is None:
                    continue
                score_text = re.sub("[A-Z.]+", "", score_elem.get_text())  # to parse stuff like "1:2 PEN." or "POST."
                if score_text == "":
                    home_score = away_score = None
                else:
                    home_score, away_score = map(int, score_text.split(":"))
            
            site_ids.append(explorer_id)
            metadata.append({
                "time_of_collection": time_of_collection,
                "site": "betexplorer",
                "game_time": datetime.fromtimestamp(game_time + 8 * 60 * 60),
                "site_id": explorer_id,
                "competition": unidecode(comp),
                "home_team": unidecode(home),
                "away_team": unidecode(away),
                "home_score": home_score,
                "away_score": away_score,
                "game_completed": game_completed,
                "game_stage": game_stage,
            })


def outcomes(log=sys.stdout):
    metadata = []
    site_ids = []
    dates = [datetime.now() - timedelta(days=i) for i in range(no_days)]
    jobs = [gevent.spawn(get_metadata, date, metadata, site_ids, log=log) for date in dates]
    gevent.wait(jobs)

    return metadata
outcomes.data_type = "outcomes"
outcomes.site = "betexplorer"
outcomes.output_types = {"home_score": "integer", "away_score": "integer",
    "game_completed": "bool", "game_stage": "bool"}
