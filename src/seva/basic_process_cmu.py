import numpy as np
import pandas as pd
import argparse
import json
import os
from tqdm import tqdm

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
    if len(date_str) == 4: # it's just year
        return pd.to_datetime(date_str, format='%Y')
    elif len(date_str) == 7: # it's year and month
        return pd.to_datetime(date_str, format='%Y-%m')
    elif len(date_str) == 10: # it's year and month and day
        return pd.to_datetime(date_str, format='%Y-%m-%d')
    elif len(date_str) == 22 or len(date_str) == 25:
        # it also contains timezone info
        return pd.to_datetime(date_str).tz_localize(None)
    else:
        print(f"Unknown date format: {date_str}")
        return None

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

# ----------------- Arguments -----------------

parser = argparse.ArgumentParser()
parser.add_argument("--data_dir", type=str, default="../../data/MovieSummaries")
parser.add_argument("--output_dir", type=str, default="../../data/MovieSummaries")
parser.add_argument("--process_movies_helper", type=str, default="./process_movies_helper.json")
parser.add_argument("--process_actors_helper", type=str, default="./process_actors_helper.json")
args = parser.parse_args()

# ----------------- Load data -----------------
with open(args.process_movies_helper, 'r') as f:
    movies_helper = json.load(f)

with open(args.process_actors_helper, 'r') as f:
    actors_helper = json.load(f)

# ----------------- Process Movies ---------------

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

# process the languages
movie_raw["languages"] = movie_raw["Movie languages"].apply(
    lambda x: process_languages(
        x, 
        skip_languages=set(movies_helper["skip_languages"])
    )
)
print("[INFO] Movie: languages are processed")

movie_raw["countries_old"] = movie_raw['Movie countries'].apply(lambda x: process_countries(x, rename_countries=movies_helper["rename_countries"]))
print("[INFO] Movie: countries are processed")

movie_raw['countries'] = movie_raw["countries_old"].apply(lambda x: process_countries_old2new(x, rename_countries=movies_helper["old_to_new"]))
print("[INFO] Movie: old2new countries are processed")

movie_raw["genres"] = movie_raw["Movie genres"].apply(lambda x: process_genres(x, rename_genres=movies_helper["rename_genres"]))
print("[INFO] Movie: genres are processed")

# ----------------- Process Actors ---------------

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

os.makedirs(args.output_dir, exist_ok=True)
movie_raw.to_csv(os.path.join(args.output_dir, "movie_processed.csv"), index=False)
char_raw.to_csv(os.path.join(args.output_dir, "character_processed.csv"), index=False)
