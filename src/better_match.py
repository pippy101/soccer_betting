"""
improved version of match.py
hopefully not as influenced by ordering of bookies
"""

from fuzzywuzzy import fuzz
from datetime import timedelta, datetime
from operator import itemgetter
from copy import copy
import bisect

class simFuncs:
    datetime_sim = lambda date1, date2: -abs(date1 - date2)
    # for loading test json files
    date_string_sim = lambda date1, date2: -abs(datetime.strptime(date1, "%Y-%m-%d %H:%M:%S") - datetime.strptime(date2, "%Y-%m-%d %H:%M:%S"))
    string_sim = lambda string1, string2: fuzz.ratio(string1.lower(), string2.lower())


thresh = {
    "game_time": {
        "threshold": timedelta(minutes=-20), "func": simFuncs.datetime_sim
    },
    "competition": {
        "threshold": 60, "func": simFuncs.string_sim
    },
    "home_team": {
        "threshold": 50, "func": simFuncs.string_sim
    },
    "away_team": {
        "threshold": 50, "func": simFuncs.string_sim
    }
}


class Match:
    """
    list of games from different sites, deemed to be the same
    """
    def __init__(self, init_game, global_id):
        self.metadata = {col: [data] for col, data in init_game.items() if col in thresh}
        self.site_ids = {init_game["site"]: init_game["site_id"]}
        self.data = [init_game]
        self.global_id = global_id
    
    @classmethod
    def from_dict(cls, dict_obj):
        base = cls(list(dict_obj.values())[0][0], list(dict_obj.keys())[0])
        for game in list(dict_obj.values())[0][1:]:
            base.add_game(game)

        return base

    def similarity(self, game_data, thresh_info: dict, field):
        """
        does not evaluate all similarities of all metadata fields
        if one field is above the threshold, it just returns that
        """
        max_sim = None
        _func = thresh_info[field]["func"]
        thresh = thresh_info[field]["threshold"]
        for metadata in self.metadata[field]:
            sim = _func(game_data, metadata)
            if sim > thresh:
                return sim
            if max_sim is None or sim > max_sim:
                max_sim = sim

        return max_sim
    
    def add_game(self, game):
        if game["site"] not in self.site_ids:
            for col in self.metadata:
                self.metadata[col].append(game[col])
            self.site_ids[game["site"]] = game["site_id"]

        self.data.append(game)
    
    def add_match(self, match):
        for game in match.data:
            self.add_game(game)
    
    def to_dict(self):
        # for json serialization
        return {self.global_id: self.data}


def lm_to_dd(matches: list[Match]) -> dict[dict]:
    """
    (list of matches to dict of dicts)
    """
    group_dict = dict()
    for match in matches:
        group_dict = group_dict | match.to_dict()
    return group_dict


def find_match(game: dict, matches: list[Match]) -> Match:
    id_sim = dict()
    site = game["site"]
    for match in matches:
        if site in match.site_ids:
            continue
        fields_sim = dict()
        for field in thresh:
            sim = match.similarity(game[field], thresh, field)
            if sim > thresh[field]["threshold"]:
                fields_sim[field] = sim
            else:
                break
        
        if len(fields_sim) == len(thresh):
            id_sim[match.global_id] = fields_sim
        else:
            if field == "game_time" and game["game_time"] < match.metadata["game_time"][0]:
                break
        
    if id_sim == {}:
        return None
    else:
        sim_getter = itemgetter("competition", "home_team", "away_team")
        total_sim = {global_id: sum(sim_getter(cert_dict)) for global_id, cert_dict in id_sim.items()}
        max_id = max(total_sim, key=total_sim.get)
        
        return max_id


def match_matches(match: Match, matches: list[Match]) -> Match:
    """
    Funny 
        match a match against a list of matches
    Not Funny
        the "lazy evaluation" of the similarity, although it speeds it up *a lot*
        kinda fucks up the max similarities calculations
        i dont know how significantly though
    """
    id_sim: dict[Match, dict] = dict()
    sites = match.site_ids.keys()
    for o_match in matches:
        if len(sites & o_match.site_ids.keys()) != 0:
            continue
        fields_sim = dict()
        for field in thresh:
            max_sim = None
            for site_data in match.metadata[field]:
                sim = o_match.similarity(site_data, thresh, field)
                if max_sim is None or sim > max_sim:
                    max_sim = sim
                """if max_sim > thresh[field]["threshold"]:
                    break"""
            if max_sim < thresh[field]["threshold"]:
                break

            fields_sim[field] = max_sim

        if len(fields_sim) == len(thresh):
            id_sim[o_match] = fields_sim

    if id_sim == {}:
        return None
    else:
        sim_getter = itemgetter("competition", "home_team", "away_team")
        total_sim = {match: sum(sim_getter(cert_dict)) for match, cert_dict in id_sim.items()}
        max_match = max(total_sim, key=total_sim.get)
        
        return max_match


def group_by_match(new_games: list[dict], min_id: int, 
    grouped_matches: list[Match] = []) -> list[dict]:
    """
    Note:
        There is a great deal of fuckery involved here 
        with object reference and such.
        This function will modify grouped_matched and the matches therein.
    """
    #TODO add the second go through of matching

    # stores a copy of the match object
    # this will probably cause much pain
    a = time.time()
    site_id_map: dict[dict[Match]] = dict()
    for match in grouped_matches:
        for game in match.data:
            if game["site"] not in site_id_map:
                site_id_map[game["site"]] = dict()
            site_id_map[game["site"]][game["site_id"]] = match
    
    old_matches = copy(grouped_matches)
    grouped_matches = sorted(grouped_matches, key=lambda x: x.metadata["game_time"][0])
    new_matches = []

    for game in new_games:
        matched = False
        matched_match = None
        if game["site"] in site_id_map:
            if game["site_id"] in site_id_map[game["site"]]:
                site_id_map[game["site"]][game["site_id"]].add_game(game)
                matched_match = site_id_map[game["site"]][game["site_id"]].global_id
                matched = True
        else:
            site_id_map[game["site"]] = dict()

        if not matched:
            matched_id = find_match(game, grouped_matches)
            
            for match in grouped_matches:
                if matched_id == match.global_id:
                    matched_match = match
                    break

            if matched_match is not None:
                matched = True
                matched_match.add_game(game)
        
        if not matched:
            min_id += 1
            matched_match = Match(game, min_id)
            bisect.insort(grouped_matches, matched_match, key=lambda x: x.metadata["game_time"][0])
            new_matches.append(matched_match)
        
        site_id_map[game["site"]][game["site_id"]] = matched_match
    print(time.time()-a)
    assert set(old_matches + new_matches) == set(grouped_matches)
    assert (set(old_matches) & set(new_matches)) == set()
    # combine old_matches and new_matches
    # this gon' be a real bitch
    """
    it no work
    i dont know why
    fix it plz
    """
    grouped_matches = old_matches
    for match in new_matches:
        matched_match = match_matches(match, grouped_matches)
        if matched_match is not None:
            matched_match.add_match(match)
        else:
            grouped_matches.append(match)

    return lm_to_dd(grouped_matches)


if __name__ == "__main__":
    import json
    import time
    import pickle
    test = json.load(open(r"test/all_data.json", "r"))
    existing_matches = pickle.load(open(r"test/match_test.pck", "rb"))
    for game in test:
        game["game_time"] = datetime.strptime(game["game_time"], "%Y-%m-%d %H:%M:%S")
    start = time.time()
    group_dict = group_by_match(test, 0, existing_matches)
    print(f"Grouping took {round(time.time()-start, 2)}s")
    print(f"{len(group_dict)} matches found")
    json.dump({k: group_dict[k] for k in sorted(group_dict, key=lambda x: len(group_dict[x]), reverse=True)}, open(r'test/better_match.json', "w"), indent=4, default=str)
    
