"""
create an empty database and put the username, password, db name etc. in conn_config.json
this script should do the rest
you can hardcode data_types in data config, if you really wish

note: for some reason, one game was able to get in the db without having a site_id
      without a runtime error or nuthin
      i dont even know *how* but it is quite worrying
"""

import gevent 
from gevent import monkey; monkey.patch_all()

from operator import itemgetter
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from copy import copy
import psycopg2
import json
import time

from Scrapers.scrapers import scrapers
from log import log
import match

NON_DATA_FIELDS = ["home_team", "away_team", "competition",
    "site_id", "site", "data_type"]
ONETIME_FIELDS = ["time_of_collection", "game_time"]
DATA_LIMIT = timedelta(days=3)


def add_list(func, list_loc, kwargs={}):
    scraper_games = func(log=log, **kwargs)
    print(f"{func.site} {func.data_type} scraped {len(scraper_games)} games")
    for game in scraper_games:
        game["data_type"] = func.data_type
        game["site"] = func.site

    list_loc += scraper_games


def get_all_data(scrapers):
    games = []
    jobs = [gevent.spawn(add_list, scraper, games) for scraper in scrapers]
    gevent.wait(jobs)
    return games


def py_sql_types(data_entry, list_str=""):
    _type = type(data_entry)
    if _type == int: return f"integer{list_str}"
    if _type == float: return f"float{list_str}"
    if _type == str: return f"text{list_str}"
    if _type == bool: return f"boolean{list_str}"
    if _type == datetime: return f"timestamp{list_str}"
    if _type == list:
        list_str += "[]"
        return py_sql_types(data_entry[0], list_str=list_str)


def flatten_dict(_dict, connector="_", name="", new_dict={}):
    for key in _dict:
        if name != "":  _name = name + connector + key
        else: _name = key

        if type(_dict[key]) != dict:
            new_dict[_name] = _dict[key]
        else:
            flatten_dict(_dict[key], connector=connector, name=_name, new_dict=new_dict)
    
    return new_dict


def none_getter(_dict, _items):
    # identical to itemgetter except
    # when an element is missing, it fills it in with None
    return tuple((None if _item not in _dict else _dict[_item] for _item in _items))


def concat_dicts(list_of_dicts):
    concat_dict = {}
    for _dict in list_of_dicts:
        concat_dict = concat_dict | _dict
    return concat_dict


def count_nulls(cur):
    sites = set([scraper.site for scraper in scrapers])
    cur.execute(f"""UPDATE global_lookup
SET no_sites=count_table.no_sites
FROM global_lookup g_lookup
JOIN (SELECT game_id, 
{'+'.join([f'CASE WHEN {site}_id IS NOT NULL THEN 1 ELSE 0 END' for site in sites])} as no_sites
FROM global_lookup) as count_table
ON g_lookup.game_id=count_table.game_id
WHERE global_lookup.game_id=count_table.game_id""")


def load_data_config(games):
    with open(r"data_config.json", "r") as fp:
        data_type_config: dict = json.load(fp)
    
    sites = set([scraper.site for scraper in scrapers])
    data_types = set([scraper.data_type for scraper in scrapers])
    datatype_tree = {}
    finished_sites = set()
    finished_data_types = set()
    for game in games:
        if game["data_type"] not in datatype_tree:
            datatype_tree[game["data_type"]] = {}
        if game["site"] not in datatype_tree[game["data_type"]]:
            datatype_tree[game["data_type"]][game["site"]] = {}

        for field in game:    
            if game[field] is not None:
                if type(game[field]) == list and len(game[field]) == 0:
                        continue
                if field not in NON_DATA_FIELDS:
                    if field not in data_type_config:
                        data_type_config[field] = py_sql_types(game[field])
                    
                    datatype_tree[game["data_type"]][game["site"]][field] = data_type_config[field]
                
                finished_sites.add(game["site"])
                finished_data_types.add(game["data_type"])
                if finished_sites ^ sites == set() \
                    and finished_data_types ^ data_types == set():
                    break
    
    all_cols = flatten_dict(datatype_tree, connector=".")
    all_cols = {k:all_cols[k] for k in all_cols \
        if k.split(".")[-1] not in ONETIME_FIELDS}
    all_cols = {k.replace(".", "_"): all_cols[k] for k in all_cols}

    with open(r"all_cols.json", "r") as fp:
        all_cols = json.load(fp)

    return data_type_config, datatype_tree, all_cols


def scrape_main(cur, setup=True):

    reset = False
    games = get_all_data(scrapers)
    
    data_type_config, datatype_tree, all_cols = load_data_config(games)

    if setup:
        ### <setup cruft> ###
        sites = set([scraper.site for scraper in scrapers])
        data_types = set([scraper.data_type for scraper in scrapers])

        site_id_type = {}
        for game in games:
            if game["site"] not in site_id_type:
                site_id_type[game["site"]] = py_sql_types(game["site_id"])
            if set(site_id_type.keys()) ^ sites == set():
                break

        if reset:
            # reset stub #
            cur.execute("""SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public'
                AND table_type='BASE TABLE';""")
            table_names = [x[0] for x in cur.fetchall()] 
            for table in table_names:
                cur.execute(f"DROP TABLE {table} CASCADE")
            # / reset stub #
        
        cur.execute("""SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
            AND table_type='BASE TABLE';""")
        table_names = [x[0] for x in cur.fetchall()] 
        
        # create match lookup table
        if "global_lookup" not in table_names:
            cur.execute("""CREATE TABLE global_lookup (
            game_id INTEGER,
            CONSTRAINT game_id PRIMARY KEY (game_id),
            home_team text not null,
            away_team text not null,
            competition text not null,
            game_time timestamp not null,
            no_sites integer,
            {site_ids})""".format(site_ids=',\n'.join([f"{site}_id {_type}" for site, _type in site_id_type.items()])))
            # add foreign key constraints once other tables have been created
            for site, _type in site_id_type.items():
                cur.execute(f"""CREATE TABLE {site}_lookup (
                game_id integer,
                FOREIGN KEY (game_id) REFERENCES global_lookup(game_id),
                {site}_id {_type},
                PRIMARY KEY ({site}_id),
                home_team text not null,
                away_team text not null,
                competition text not null,
                game_time timestamp not null)""")
                
            cur.execute("""ALTER TABLE global_lookup
            {}""".format(
                ",\n".join([f"ADD FOREIGN KEY ({site}_id) REFERENCES {site}_lookup({site}_id)" for site in sites])
            ))

        if "data" not in table_names:
            cur.execute("""CREATE TABLE data (
            game_id integer,
            FOREIGN KEY (game_id) REFERENCES global_lookup(game_id),
            time_of_collection timestamp,
            PRIMARY KEY (game_id, time_of_collection),
            game_time timestamp,
            {})""".format(',\n'.join([f"{k} {v}" for k, v in all_cols.items()])))
        else:
            # fill in extra column names
            cur.execute("SELECT * FROM data LIMIT 0")
            columns = set([desc[0] for desc in cur.description])
            missing_columns = columns ^ set(all_cols.keys()) ^ set(["game_id", "time_of_collection", "game_time"])
            missing_columns = [col for col in all_cols.keys() if col not in columns and col not in ["game_id", "time_of_collection", "game_time"]]
            if missing_columns != []:
                cur.execute("""ALTER TABLE data
                {}""".format(',\n'.join([f"ADD COLUMN {col} {all_cols[col]}" for col in missing_columns])))
        
        if "small_data" not in table_names:
            cur.execute("""CREATE TABLE small_data (
            game_id integer,
            FOREIGN KEY (game_id) REFERENCES global_lookup(game_id),
            time_of_collection timestamp,
            PRIMARY KEY (game_id, time_of_collection),
            game_time timestamp,
            {})""".format(',\n'.join([f"{k} {v}" for k, v in all_cols.items()])))
        else:
            # fill in extra column names
            cur.execute("SELECT * FROM data LIMIT 0")
            columns = set([desc[0] for desc in cur.description])
            missing_columns = columns ^ set(all_cols.keys()) ^ set(["game_id", "time_of_collection", "game_time"])
            missing_columns = [col for col in all_cols.keys() if col not in columns and col not in ["game_id", "time_of_collection", "game_time"]]
            if missing_columns != []:
                cur.execute("""ALTER TABLE small_data
                {}""".format(',\n'.join([f"ADD COLUMN {col} {all_cols[col]}" for col in missing_columns])))
        
        if 'outcomes' not in table_names:
            cur.execute("""CREATE TABLE outcomes (
                game_id integer,
                PRIMARY KEY (game_id),
                FOREIGN KEY (game_id) REFERENCES global_lookup(game_id),
                home_score smallint,
                away_score smallint)""")
        
        with open(r"data_config.json", "w") as fp:
            json.dump(data_type_config, fp, indent=4)
    
    # find already matched games
    unmatched_games = []
    existing_games = dict()
    site_ids = dict()
    for game in games:
        game_id = None
        if game["site"] not in site_ids:
            site_ids[game["site"]] = dict()
        if game["site_id"] in site_ids[game["site"]]:
            game_id = site_ids[game["site"]][game["site_id"]]

        if game_id is None:
            cur.execute(f"""SELECT game_id FROM global_lookup
            WHERE {game['site']}_id = '{game["site_id"]}'""")
            fetched = cur.fetchall()
            if len(fetched) != 0:
                game_id = fetched[0][0]
                site_ids[game["site"]][game["site_id"]] = game_id
        
        if game_id is None:
            metadata_cols = ["game_id", "home_team", "away_team", "competition", "game_time"]
            cur.execute(f"""SELECT {','.join(metadata_cols)} FROM global_lookup
            WHERE game_time < (TIMESTAMP '{game['game_time'] + timedelta(minutes=20)}')
            AND game_time > (TIMESTAMP '{game['game_time'] - timedelta(minutes=20)}')""")
            tup_games = cur.fetchall()
            dict_games = [{k: v for k, v in zip(metadata_cols, _game)} for _game in tup_games]
            l_games = [(game["game_id"], game) for game in dict_games]
            game_id = match.match_game(game, l_games)
        
        if game_id is not None:
            if game_id not in existing_games:
                existing_games[game_id] = [game]
            else:
                existing_games[game_id].append(game)
        else:
            unmatched_games.append(game)

    no_existing_games = len(existing_games)
    # match the rest
    cur.execute("SELECT count(*) FROM global_lookup")
    min_id = cur.fetchall()[0][0]
    matched_games = match.group_by_match(unmatched_games, min_id, existing_matches=copy(existing_games))
    print(f"Found {len(matched_games)-no_existing_games:,} new games and {no_existing_games:,} inserted games")
    new_games = {k:v for k, v in matched_games.items() if k not in existing_games}
    # update _id lookup table and metadata table #
    # insert game_ids into global_lookup
    init_insert_cols = ["game_id", "home_team", "away_team", "competition", "game_time"]
    query = "INSERT INTO global_lookup ({}) VALUES %s".format(','.join(init_insert_cols))
    col_getter = itemgetter(*["home_team", "away_team", "competition", "game_time"])
    values = [(game_id, *col_getter(_games[0])) for game_id, _games in new_games.items()]
    execute_values(cur, query, values)
    # insert into sites
    site_getter = itemgetter(*["site_id", "home_team", "away_team", "competition", "game_time"])
    for site in sites:
        site_cols = ["game_id", f"{site}_id", "home_team", "away_team", "competition", "game_time"]
        site_query = "INSERT INTO {site}_lookup ({cols}) VALUES %s ON CONFLICT DO NOTHING".format(site=site, cols=','.join(site_cols))
        site_games = []
        for game_id, _games in matched_games.items():
            for game in _games:
                if game["site"] == site:
                    site_games.append((game_id, *site_getter(game)))
                    break

        execute_values(cur, site_query, site_games)
    # insert keys back into global_lookup
    # this could (*should*) probably be done with a trigger or some such
    for site in sites:
        cur.execute(f"""UPDATE global_lookup
        SET {site}_id=site_table.{site}_id
        FROM global_lookup g_lookup
        JOIN {site}_lookup site_table
        ON g_lookup.game_id=site_table.game_id
        WHERE global_lookup.game_id=site_table.game_id""")

    # put completed scores in table
    outcomes = []
    for game_id, _games in matched_games.items():
        for game in _games:
            if game["site"]=="betexplorer" and game["game_completed"]:
                outcomes.append({"game_id": game_id, **game})
    outcome_cols = ["game_id", "home_score", "away_score"]
    outcome_query = "INSERT INTO outcomes ({}) VALUES %s ON CONFLICT DO NOTHING".format(','.join(outcome_cols))
    # i love you, itemgetter
    col_getter = itemgetter(*outcome_cols)
    values = [col_getter(game) for game in outcomes]
    execute_values(cur, outcome_query, values)

    # put the rest in 
    # remove completed score_data
    matched_games = {global_id: list(filter(lambda game: game["site"]!="betexplorer" or (game["site"]=="betexplorer" and not game["game_completed"]), _games)) \
        for global_id, _games in matched_games.items()}
    # concat dicts, and fetch data
    data = []
    small_data = []
    columns = ["game_id", 'time_of_collection', "game_time", *all_cols]
    for game_id, _games in matched_games.items():
        _game = {}
        _game["game_id"] = game_id
        if len(_games) == 0:
            continue
        for site_data in _games:
            for field in site_data:
                data_type = site_data["data_type"]
                site = site_data["site"]
                if field in ONETIME_FIELDS:
                    _game[field] = site_data[field]
                elif field != "data_type" and field != "site":
                    _game[f"{data_type}_{site}_{field}"] = site_data[field]
        
        none_got = none_getter(_game, columns)
        data.append(none_got)
        if _game["game_time"] - game["time_of_collection"] < DATA_LIMIT:
            small_data.append(none_got)

    insert_query = "INSERT INTO data ({}) VALUES %s ON CONFLICT DO NOTHING".format(','.join(columns))
    execute_values(cur, insert_query, data)
    insert_query = "INSERT INTO small_data ({}) VALUES %s ON CONFLICT DO NOTHING".format(','.join(columns))
    execute_values(cur, insert_query, small_data)

    count_nulls(cur)

    print("--- Inserted data ---")


with open(r"conn_config.json", "r", encoding="utf-8") as fp:
    conn_config = json.load(fp)


try:
    conn = psycopg2.connect(**conn_config)
    cur = conn.cursor()

    LOOP_LENGTH = 10 * 60
    while True:
        start = time.time()
        scrape_main(cur)
        conn.commit()
        time_taken = time.time() - start
        sleep_time = LOOP_LENGTH-time_taken
        while sleep_time > 0:
            print(f"Waiting {timedelta(seconds=int(sleep_time))}", end='\r')
            time.sleep(1)
            sleep_time -=1
        print('\n')

except (Exception, psycopg2.DatabaseError) as error:
    if conn is not None:
        conn.close()

    raise error
