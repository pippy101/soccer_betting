"""
predictions outputs a list of forebet's predictions
NO_DAYS constant controls how many days into the future to scrape
"""

from datetime import datetime, timedelta
from unidecode import unidecode
import gevent
import requests
import sys

NO_DAYS = 2
headers = {
    "accept-encoding": "gzip, deflate, br",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36",
    "content-type": "application/json",
}
pred_url = "https://www.forebet.com/scripts/getrs.php?ln=en&tp=1x2&in={iso_data}&ord=0&tz=+480&tzs=0&tze=0"


def parse_forebet(url, list_loc, log):
    try:
        with requests.get(url, headers=headers) as resp:
            if resp.status_code == 200:
                time_of_collection = int(datetime.utcnow().timestamp())
                raw_data = resp.json()
            else:
                log.write(f"Forebet scraper failed with status code: {resp.status_code}, link: {resp.url}")
    except requests.exceptions.ConnectionError:
        log.write("Connection aborted error in forebet_scraper")
        return
    
    cruft_fields = {"time_of_collection": time_of_collection, "site": "forebet"}
    for match in raw_data[0]:
        match_datetime = datetime.strptime(match["DATE_BAH"], "%Y-%m-%d %H:%M:%S").timestamp() + 8 * 60 * 60
        forebet_id = match["id"]
        home = unidecode(match["HOST_NAME"])
        away = unidecode(match["GUEST_NAME"])
        league_info = raw_data[1][match["league_id"]]
        comp = f"{league_info[0]} {league_info[1]}"

        round_no = int(match["Round"])
        home_pred = int(match["Pred_1"])
        away_pred = int(match["Pred_2"])
        draw_pred = int(match["Pred_X"])
        avg_goals = float(match["goalsavg"])

        list_loc.append({
            "home_team": home,
            "away_team": away,
            "competition": comp,
            "site_id": forebet_id,
            "match_datetime": match_datetime,
            "round_no": round_no,
            "home_pred": home_pred,
            "away_pred": away_pred,
            "draw_pred": draw_pred,
            "avg_goals": avg_goals,
            **cruft_fields
        })


def predictions(log=sys.stdout):
    scrape_dates = [(datetime.now()+timedelta(days=i)).strftime("%Y-%m-%d") for i in range(NO_DAYS)]
    urls = [pred_url.format(iso_data=iso_date) for iso_date in scrape_dates]
    
    predictions = []
    jobs = [gevent.spawn(parse_forebet, url, predictions, log=log) for url in urls]
    gevent.wait(jobs)
    
    return predictions
