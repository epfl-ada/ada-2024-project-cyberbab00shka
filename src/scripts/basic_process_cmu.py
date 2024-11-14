import numpy as np
import pandas as pd
import argparse
import json
import os
from tqdm import tqdm

# ----------------- Arguments -----------------

parser = argparse.ArgumentParser()
parser.add_argument("--data_dir", type=str, default="../../data/MovieSummaries", help="path to MovieSummaries")
parser.add_argument("--output_dir", type=str, default="../../data/MovieSummaries", help="path where to store outputs")
parser.add_argument("--process_movies_helper", type=str, default="./scripts_json_helpers/process_movies_helper.json", help="(by default it is in the same folder)")
parser.add_argument("--process_actors_helper", type=str, default="./scripts_json_helpers/process_actors_helper.json", help="(by default it is in the same folder)")
parser.add_argument("--process_ethnicities_helper", type=str, default="./scripts_json_helpers/process_ethnicities_helper.json", help="(by default it is in the same folder)")
parser.add_argument("--process_type", choices=["all", "movies", "characters"], default="all")
args = parser.parse_args()

# ----------------- Functions -----------------

def process_date(date_str, change_date=None):
    """
    Function processes date string and returns datetime object
    """
    if change_date is None:
        change_date = {}
    if date_str in change_date:
        date_str = change_date[date_str]
    if pd.isna(date_str):
        return None
    result = None
    if len(date_str) == 4: # it's just year
        result = pd.to_datetime(date_str, format='%Y')
    elif len(date_str) == 7: # it's year and month
        result = pd.to_datetime(date_str, format='%Y-%m')
    elif len(date_str) == 10: # it's year and month and day
        result = pd.to_datetime(date_str, format='%Y-%m-%d')
    elif len(date_str) == 22 or len(date_str) == 25:
        # it also contains timezone info
        result = pd.to_datetime(date_str).tz_localize(None)
    else:
        print(f"Unknown date format: {date_str}")
        return None
    return result.strftime("%Y-%m-%d")

# ----------------------------------------------------------------

def process_languages(languages_raw, skip_languages=None):
    if skip_languages is None:
        skip_languages = set()
    if pd.isna(languages_raw):
        return []
    
    # get the languages
    languages_raw = eval(languages_raw)
    languages_result = set()
    
    # process them
    for lang_id, lang in languages_raw.items():
        if lang in skip_languages:
            continue
        lang = lang.lower()
        if "languages" in lang:
            lang = lang.replace("language", "").strip()
        elif "language" in lang:
            lang = lang.replace("language", "").strip()
        elif "Language" in lang:
            lang = lang.replace("Language", "").strip()
        languages_result.add(lang)
    return list(languages_result)

# ----------------------------------------------------------------

def process_countries(countries_raw, rename_countries=None):
    if rename_countries is None:
        rename_countries = {}
    if pd.isna(countries_raw):
        return []
    # get the countries
    countries_result = set()
    countries = eval(countries_raw)
    # process them
    for country in countries.values():
        countries_result.update(rename_countries.get(country, [country]))
    return list(countries_result)


def process_countries_old2new(countries, rename_countries=None):
    if rename_countries is None:
        rename_countries = {}
    if countries is None:
        return []
    countries_result = set()
    for country in countries:
        countries_result.add(rename_countries.get(country, country))
    return list(countries_result)

# ----------------------------------------------------------------

def process_genres(genre_raw, rename_genres=None):
    if rename_genres is None:
        rename_genres = {}
    if pd.isna(genre_raw):
        return []
    # get the genres
    genres_result = set()
    genres = eval(genre_raw)
    # process them
    for genre in genres.values():
        genres_result.update(rename_genres.get(genre, [genre]))
    return list(genres_result)

# ----------------- Process Movies ---------------

with open(args.process_movies_helper, 'r') as f:
    movies_helper = json.load(f)

if args.process_type in ["all", "movies"]:

    movie_raw = pd.read_csv(
        os.path.join(args.data_dir, 'movie.metadata.tsv'), 
        sep='\t', 
        header=None
    )

    movie_raw.rename(columns={
        0: 'Wikipedia movie ID',
        1: 'Freebase movie ID',
        2: 'Movie name',
        3: 'Movie release date',
        4: 'Movie box office revenue',
        5: 'Movie runtime',
        6: 'Movie languages',
        7: 'Movie countries',
        8: 'Movie genres'
    }, inplace=True)

    movie_raw["languages"] = movie_raw["Movie languages"].apply(
        lambda x: process_languages(
            x, 
            skip_languages=set(movies_helper["skip_languages"])
        )
    )
    print("[INFO] Movie: languages are processed")

    movie_raw["movie_release_date"] = pd.to_datetime(
        movie_raw["Movie release date"].apply(lambda x: process_date(x, change_date=movies_helper["change_date"]))
    )
    print("[INFO] Movie: releases dates are processed")

    movie_raw["countries_old"] = movie_raw['Movie countries'].apply(lambda x: process_countries(x, rename_countries=movies_helper["rename_countries"]))
    print("[INFO] Movie: countries are processed")

    movie_raw['countries'] = movie_raw["countries_old"].apply(lambda x: process_countries_old2new(x, rename_countries=movies_helper["old_to_new"]))
    print("[INFO] Movie: old2new countries are processed")

    movie_raw["genres"] = movie_raw["Movie genres"].apply(lambda x: process_genres(x, rename_genres=movies_helper["rename_genres"]))
    print("[INFO] Movie: genres are processed")

    movie_raw = movie_raw.drop(columns=["Movie release date", "Movie languages", "Movie countries", "Movie genres"])

# ----------------- Process Actors ---------------

if args.process_type in ["all", "characters"]:
    with open(args.process_actors_helper, 'r') as f:
        actors_helper = json.load(f)

    char_raw = pd.read_csv(
        os.path.join(args.data_dir, 'character.metadata.tsv'), 
        sep='\t', 
        header=None
    )
    char_raw.rename(columns={
        0: "Wikipedia movie ID",
        1: "Freebase movie ID",
        2: "Movie release date",
        3: "Character name",
        4: "Actor date of birth",
        5: "Actor gender",
        6: "Actor height (in meters)",
        7: "Actor ethnicity (Freebase ID)",
        8: "Actor name",
        9: "Actor age at movie release",
        10: "Freebase character/actor map ID",
        11: "Freebase character ID",
        12: "Freebase actor ID",
    }, inplace=True)

    char_raw['actor_date_of_birth'] = char_raw['Actor date of birth'].apply(lambda x: process_date(
        x, 
        change_date=actors_helper["change_date"]
    ))

    print("[INFO] Characters: (Actor date of birth) are processed")

    char_raw['movie_release_date'] = char_raw['Movie release date'].apply(lambda x: process_date(
        x, 
        change_date=movies_helper["change_date"]
    ))

    print("[INFO] Characters: (Movie release date) are processed")

    # remove actors when birth date is later than movie release date
    char_raw = char_raw[char_raw['actor_date_of_birth'] < char_raw['movie_release_date']].copy()

    # process ethnities
    if os.path.exists(args.process_ethnicities_helper):
        print("Process Ethnicities")
        with open(args.process_ethnicities_helper) as f:
            ethnicity_process_helper = json.load(f)
        
        all_ethnicities = set(char_raw["Actor ethnicity (Freebase ID)"].values)
        all_ethnicities_found = set(list(ethnicity_process_helper["fb2ethnicity"].keys())) & all_ethnicities
        # create a pd table to concat

        ethnicity_pd = []
        for ethn_id in all_ethnicities_found:
            label = ethnicity_process_helper["fb2ethnicity"][ethn_id]
            if not label in ethnicity_process_helper["rename_ethnicities"]:
                continue
            result = {
                "ethn_id": ethn_id,
                "ethn_name": ethnicity_process_helper["rename_ethnicities"][label],
            }
            result["race"] = ethnicity_process_helper["ethnicity2race"][result["ethn_name"]]
            ethnicity_pd.append(result)
        ethnicity_pd = pd.DataFrame(ethnicity_pd)
        char_raw = char_raw\
            .merge(ethnicity_pd, left_on="Actor ethnicity (Freebase ID)", right_on="ethn_id", how="left")\
            .drop(columns=["ethn_id", "Actor date of birth", "Movie release date", "Actor age at movie release"])

# save the processed data
os.makedirs(args.output_dir, exist_ok=True)
if args.process_type in ["all", "movies"]:
    movie_raw.to_csv(os.path.join(args.output_dir, "movie_processed.csv"), index=False)
if args.process_type in ["all", "characters"]:
    char_raw.to_csv(os.path.join(args.output_dir, "character_processed.csv"), index=False)
