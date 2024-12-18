import argparse
import logging
import os
import random
import urllib.error

import imdb
import joblib
import pandas as pd
import requests
from tqdm import tqdm
from tqdm_joblib import tqdm_joblib


def test_proxy(proxy, timeout=10):
    """
    Test if a proxy is working
    """
    proxies = {"http": f"http://{proxy}", "https": f"https://{proxy}"}
    try:
        response = requests.get(
            "http://ipinfo.io/json", proxies=proxies, timeout=timeout
        )
        return response.status_code == 200
    except (requests.ConnectionError, requests.Timeout):
        return False


def get_working_proxies(proxy_list, max_proxies=10):
    """
    Filter and return a list of working proxies
    """
    working_proxies = []
    for proxy in proxy_list:
        if test_proxy(proxy):
            working_proxies.append(proxy)
            if len(working_proxies) >= max_proxies:
                break
    return working_proxies


def process_character_row(row_dict, proxies=None):
    """
    Process a character row with optional proxy support
    """
    imdb_search = imdb.IMDb()
    processed_row = row_dict.copy()

    if pd.isna(processed_row.get("IMDB movie ID")):
        return processed_row

    try:
        if proxies:
            proxy = random.choice(proxies)
            imdb_search.set_proxy(
                {"http": f"http://{proxy}", "https": f"https://{proxy}"}
            )

        actor_search_results = imdb_search.search_person(processed_row["Actor name"])
        if actor_search_results:
            actor = actor_search_results[0]
            imdb_search.update(actor)
            processed_row["IMDB actor ID"] = f"nm{actor.personID}"

        movie_id = processed_row["IMDB movie ID"].replace("tt", "")
        movie = imdb_search.get_movie(movie_id)

        for cast_member in movie.get("cast", []):
            if (
                processed_row["Actor name"].lower()
                in cast_member.get("name", "").lower()
            ):
                character_name = cast_member.currentRole
                if (
                    character_name
                    and processed_row["Character name"].lower()
                    in str(character_name).lower()
                ):
                    processed_row["IMDB character ID"] = (
                        f"ch{hash(str(character_name)) % 10000}"
                    )
                    break

    except (urllib.error.HTTPError, requests.exceptions.RequestException) as e:
        logging.warning(f"Error processing row (proxy might be blocked): {str(e)}")
    except Exception as e:
        logging.warning(f"Error processing row: {str(e)}")

    return processed_row


def add_imdb_identifiers_character_parallel(
    character_input_file,
    movie_input_file,
    output_dir,
    proxy_list=None,
    n_rows: int = None,
    n_jobs: int = -1,
):
    """
    Add IMDB identifiers with proxy support and parallel processing
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    try:
        working_proxies = get_working_proxies(proxy_list) if proxy_list else None

        logging.info(f"Using {len(working_proxies or [])} working proxies")

        df_characters = pd.read_csv(
            character_input_file,
            dtype={"Wikipedia movie ID": str, "Freebase movie ID": str},
            nrows=n_rows,
        )
        df_movies = pd.read_csv(
            movie_input_file,
            dtype={"Wikipedia movie ID": str, "Freebase movie ID": str},
            nrows=n_rows,
        )

        movie_id_map = df_movies.set_index("Wikipedia movie ID")["IMDB movie ID"]

        df_characters["IMDB movie ID"] = df_characters["Wikipedia movie ID"].map(
            movie_id_map
        )
        df_characters["IMDB actor ID"] = ""
        df_characters["IMDB character ID"] = ""

        with tqdm_joblib(
            tqdm(desc="Processing Characters", total=len(df_characters))
        ) as progress_bar:
            processed_rows = joblib.Parallel(n_jobs=n_jobs)(
                joblib.delayed(process_character_row)(
                    row.to_dict(), proxies=working_proxies
                )
                for _, row in df_characters.iterrows()
            )

        df_processed = pd.DataFrame(processed_rows)

        columns = list(df_processed.columns)
        imdb_id_index = columns.index("Freebase movie ID") + 1

        columns_to_move = ["IMDB movie ID", "IMDB actor ID", "IMDB character ID"]
        for col in columns_to_move:
            columns.insert(imdb_id_index, columns.pop(columns.index(col)))

        df_processed = df_processed[columns]

        df_processed.to_csv(output_dir, index=False)

        total_rows = len(df_processed)
        matched_movie_rows = df_processed["IMDB movie ID"].notna().sum()
        matched_actor_rows = df_processed["IMDB actor ID"].notna().sum()
        matched_character_rows = df_processed["IMDB character ID"].notna().sum()

        logging.info(f"Total rows: {total_rows}")
        logging.info(
            f"Rows with matched IMDB Movie ID: {matched_movie_rows} ({matched_movie_rows/total_rows*100:.2f}%)"
        )
        logging.info(
            f"Rows with matched IMDB Actor ID: {matched_actor_rows} ({matched_actor_rows/total_rows*100:.2f}%)"
        )
        logging.info(
            f"Rows with matched IMDB Character ID: {matched_character_rows} ({matched_character_rows/total_rows*100:.2f}%)"
        )

        print(f"Successfully added IMDB IDs. Output saved to {output_dir}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_dir",
        type=str,
        default="../data/MovieSummaries",
        help="path to MovieSummaries",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="../data/MovieSummaries",
        help="path where to store outputs",
    )
    parser.add_argument(
        "--n_rows",
        type=int,
        default=100,
        help="number of rows to process (default: 100, use -1 for all rows)",
    )
    parser.add_argument(
        "--proxies",
        type=str,
        help="comma-separated list of proxy servers (ip:port)",
    )
    parser.add_argument(
        "--n_jobs",
        type=int,
        default=-1,
        help="number of parallel jobs to run (default: -1, use all cores)",
    )

    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    proxy_list = args.proxies.split(",") if args.proxies else None

    character_input_file = os.path.join(args.data_dir, "character_processed.csv")
    movie_input_file = os.path.join(args.data_dir, "movie_processed_with_imdb.csv")
    output_file = os.path.join(args.output_dir, "character_processed_with_imdb.csv")

    print(f"Character file: {character_input_file}")
    print(f"Movie file: {movie_input_file}")
    print(f"Output file: {output_file}")
    print(f"Processing {args.n_rows if args.n_rows != -1 else 'all'} rows")
    print(f"Using {args.n_jobs} parallel jobs")
    print(f"Proxy servers: {proxy_list or 'None'}")

    n_rows = None if args.n_rows == -1 else args.n_rows

    add_imdb_identifiers_character_parallel(
        character_input_file,
        movie_input_file,
        output_file,
        proxy_list=proxy_list,
        n_rows=n_rows,
        n_jobs=args.n_jobs,
    )


if __name__ == "__main__":
    main()
