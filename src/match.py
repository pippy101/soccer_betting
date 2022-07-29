"""
functions for finding the same game (matches) between multiple sites
match_game matches one game to a list of global_ids and games (outputs the global_id)
group_by_match groups a list of games from different sites (uses match_game)
"""

from fuzzywuzzy import fuzz
from itertools import groupby

class simFuncs:
    datetime_sim = lambda date1, date2: -abs(date1 - date2) / 60
    string_sim = lambda string1, string2: fuzz.ratio(string1.lower(), string2.lower())


thresh = {
    "match_datetime": {
        "threshold": -20, "func": simFuncs.datetime_sim
    },
    "competition": {
        "threshold": 60, "func": simFuncs.string_sim
    },
    "home_team": {
        "threshold": 60, "func": simFuncs.string_sim
    },
    "away_team": {
        "threshold": 50, "func": simFuncs.string_sim
    }
}


def match_game(game: dict, l_games: list[tuple[int, dict]]):
    """
    l_games is a list of (global_id, game data)
    game is one game that needs a global_id
    returns None for no match found
        or the most similar game's id
    """
    # similarity of each field of each id
    id_sim: dict[dict[int]] = dict()
    for global_id, poss_game in l_games:
        fields_sim = {}
        for field in thresh:
            sim_thresh = thresh[field]["threshold"]
            sim_func = thresh[field]["func"]

            similarity = sim_func(game[field], poss_game[field])
            if similarity > sim_thresh:
                fields_sim[field] = similarity
            else:
                break
        # if one of the field's threshold isn't met, skip
        if len(fields_sim) == len(thresh):
            id_sim[global_id] = fields_sim
    
    if len(id_sim) == 0:
        return None
    else:
        total_sim = {global_id: sum(cert_dict.values()) for global_id, cert_dict in id_sim.items()}
        max_id = max(total_sim, key=total_sim.get)
        
        return max_id


def group_by_match(data_points: list[dict], min_id: int):
    """
    takes in list of data-points 
    then groups them by whatever it thinks are the same matches
    assigns it a 'global_id', which is just an incrementing integer
        of which you can assign the starting point of the incrementing

    note: string matching is by no means perfect, 
        if there is a better method replace the simFuncs or even the match_game function
    """
    grouped_games: dict[int, list[dict]] = dict()
    # dict for keeping track of duplicates/entries from same site
    site_ids: dict[str or int, int] = dict()
    
    for site, games in groupby(data_points, key=lambda x: x["site"]):
        for game in games:
            # match with games in grouped_games
            matched_id = None
            if game["site_id"] in site_ids:
                matched_id = site_ids[game["site_id"]]
            if matched_id is None:
                matched_id = match_game(game, 
                    [(global_id, _games[0]) for global_id, _games in grouped_games.items() if site not in [x["site"] for x in _games]])
            # increment min_id if no match found
            if matched_id is None:
                min_id += 1
                matched_id = min_id
                grouped_games[matched_id] = []

            site_ids[game["site_id"]] = matched_id
            grouped_games[matched_id].append(game)
    
    return grouped_games


if __name__ == "__main__":
    import json
    test = json.load(open("all_odds.json", "r"))
    json.dump(group_by_match(test, 0), open('group_test.json', "w"), indent=4)