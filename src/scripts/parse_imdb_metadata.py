import argparse
import difflib
import json
import logging
import os
import re
import time
from typing import List, Optional

import imdb
import joblib
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm
from tqdm_joblib import tqdm_joblib


def test_proxy(proxy: str, timeout: int = 10) -> bool:
    """
    Test if a proxy is working

    Args:
        proxy (str): Proxy in format 'ip:port'
        timeout (int): Timeout for proxy connection

    Returns:
        bool: True if proxy is working, False otherwise
    """
    proxies = {"http": f"http://{proxy}", "https": f"https://{proxy}"}
    try:
        response = requests.get(
            "http://ipinfo.io/json", proxies=proxies, timeout=timeout
        )
        return response.status_code == 200
    except (requests.ConnectionError, requests.Timeout, requests.RequestException):
        return False


def get_working_proxies(proxy_list: List[str], max_proxies: int = 10) -> List[str]:
    """
    Filter and return a list of working proxies

    Args:
        proxy_list (list): List of proxy strings in 'ip:port' format
        max_proxies (int): Maximum number of working proxies to return

    Returns:
        list: List of working proxies
    """
    working_proxies = []
    for proxy in proxy_list:
        if test_proxy(proxy):
            working_proxies.append(proxy)
            if len(working_proxies) >= max_proxies:
                break
    return working_proxies


class IMDBIDRetriever:
    def __init__(
        self,
        input_file: str,
        output_file: str,
        checkpoint_file: str,
        n_rows: int,
        proxies: Optional[List[str]] = None,
    ):
        """
        Initialize the IMDB ID retrieval process with checkpoint and proxy support
        """
        self.imdb_search = imdb.IMDb()
        self.working_proxies = get_working_proxies(proxies) if proxies else []
        logging.info(f"Initialized with {len(self.working_proxies)} working proxies")

        try:
            if n_rows is None:
                self.df = pd.read_csv(input_file)
                logging.info(f"Successfully loaded ALL {len(self.df)} rows")
            else:
                self.df = pd.read_csv(input_file, nrows=n_rows)
                logging.info(f"Successfully loaded {len(self.df)} rows")
        except Exception as e:
            logging.error(f"Error loading input file: {e}")
            raise

        self.output_file = output_file
        self.checkpoint_file = checkpoint_file
        self.n_rows = n_rows

        self.checkpoint = self._load_checkpoint()

    def _load_checkpoint(self) -> dict:
        """
        Load existing checkpoint data
        """
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, "r") as f:
                    checkpoint = json.load(f)
                    logging.info(f"Loaded checkpoint from {self.checkpoint_file}")
                    return checkpoint
        except Exception as e:
            logging.error(f"Error loading checkpoint: {e}")

        return {"last_processed_index": -1, "processed_ids": {}}

    def _save_checkpoint(self, index: int, imdb_id: Optional[str]) -> None:
        """
        Save checkpoint information
        """
        try:
            self.checkpoint["last_processed_index"] = index
            if imdb_id:
                movie_name = self.df.loc[index, "Movie name"]
                self.checkpoint["processed_ids"][movie_name] = imdb_id

            with open(self.checkpoint_file, "w") as f:
                json.dump(self.checkpoint, f, indent=4)
        except Exception as e:
            logging.error(f"Error saving checkpoint: {e}")

    def clean_movie_name(self, name: str) -> str:
        """
        Clean and standardize movie name for better matching
        """
        name = re.sub(r"[^\w\s]", "", name.lower())
        name = " ".join(name.split())
        return name

    def find_imdb_id(self, row: pd.Series) -> Optional[str]:
        """
        Find IMDB ID for a movie based on multiple matching strategies
        """
        try:
            movie_name = row["Movie name"]
            release_year = str(row["movie_release_date"])[:4]
            clean_name = self.clean_movie_name(movie_name)

            if self.working_proxies:
                proxy = np.random.choice(self.working_proxies)
                self.imdb_search.set_proxy(
                    {"http": f"http://{proxy}", "https": f"https://{proxy}"}
                )

            search_results = self.imdb_search.search_movie(movie_name)

            matched_movies = []
            for movie in search_results:
                search_name = self.clean_movie_name(movie["title"])

                movie_year = str(movie.get("year", ""))

                similarity_ratio = difflib.SequenceMatcher(
                    None, clean_name, search_name
                ).ratio()

                if similarity_ratio > 0.8 and movie_year == release_year:
                    matched_movies.append((similarity_ratio, movie))

            if matched_movies:
                best_match = max(matched_movies, key=lambda x: x[0])
                imdb_id = f"tt{best_match[1].getID()}"
                logging.info(f"Matched {movie_name} with IMDB ID: {imdb_id}")

                time.sleep(1)
                return imdb_id

            logging.warning(f"No IMDB ID found for {movie_name}")
            return None

        except Exception as e:
            logging.error(f"Error finding IMDB ID for {movie_name}: {e}")
            return None

    def process_imdb_ids(self) -> None:
        """
        Process and add IMDB IDs to the dataframe with checkpoint support
        """
        try:
            start_index = self.checkpoint["last_processed_index"] + 1

            if "IMDB movie ID" not in self.df.columns:
                self.df["IMDB movie ID"] = None

            for movie_name, imdb_id in self.checkpoint.get("processed_ids", {}).items():
                self.df.loc[self.df["Movie name"] == movie_name, "IMDB movie ID"] = (
                    imdb_id
                )

            for index in tqdm(
                range(start_index, len(self.df)), desc="Processing Movies"
            ):
                if pd.isna(self.df.loc[index, "IMDB movie ID"]):
                    imdb_id = self.find_imdb_id(self.df.loc[index])
                    self.df.loc[index, "IMDB movie ID"] = imdb_id

                self._save_checkpoint(index, imdb_id)

            imdb_id_validity = self.df["IMDB movie ID"].notna().mean()
            logging.info(
                f"Retrieved IMDB IDs for {imdb_id_validity * 100:.2f}% of rows"
            )

            self.df.to_csv(self.output_file, index=False)
            logging.info(f"Saved processed data to {self.output_file}")

        except Exception as e:
            logging.error(f"Error processing IMDB IDs: {e}")
            self.df.to_csv(self.output_file, index=False)


def clean_movie_name(name: str) -> str:
    """
    Clean and standardize movie name for better matching
    """
    name = re.sub(r"[^\w\s]", "", name.lower())
    name = " ".join(name.split())
    return name


def process_movie_row(row_dict, proxies: Optional[List[str]] = None):
    """
    Process a single movie row to find IMDB ID with proxy support
    """
    imdb_search = imdb.IMDb()
    movie_name = row_dict["Movie name"]
    release_year = str(row_dict["movie_release_date"])[:4]
    clean_name = clean_movie_name(movie_name)

    try:
        if proxies:
            proxy = np.random.choice(proxies)
            imdb_search.set_proxy(
                {"http": f"http://{proxy}", "https": f"https://{proxy}"}
            )

        search_results = imdb_search.search_movie(movie_name)
        matched_movies = []
        for movie in search_results:
            search_name = clean_movie_name(movie["title"])
            movie_year = str(movie.get("year", ""))
            similarity_ratio = difflib.SequenceMatcher(
                None, clean_name, search_name
            ).ratio()
            if similarity_ratio > 0.8 and movie_year == release_year:
                matched_movies.append((similarity_ratio, movie))

        if matched_movies:
            best_match = max(matched_movies, key=lambda x: x[0])
            imdb_id = f"tt{best_match[1].getID()}"
            time.sleep(1)
            return (row_dict["index"], imdb_id)
        else:
            return (row_dict["index"], None)
    except Exception as e:
        logging.error(f"Error processing {movie_name}: {e}")
        return (row_dict["index"], None)


def add_imdb_ids_parallel(
    input_file,
    output_file,
    n_rows: int,
    n_jobs: int,
    proxies: Optional[List[str]] = None,
):
    """
    Add IMDB IDs to the dataframe using parallel processing with proxy support
    """
    working_proxies = get_working_proxies(proxies) if proxies else []
    logging.info(f"Using {len(working_proxies)} working proxies")

    df = pd.read_csv(input_file, nrows=n_rows)
    if "IMDB movie ID" not in df.columns:
        df["IMDB movie ID"] = None
    df["index"] = df.index
    rows_to_process = df.to_dict("records")

    with tqdm_joblib(
        tqdm(desc="Processing Movies", total=len(rows_to_process))
    ) as progress_bar:
        results = joblib.Parallel(return_as="list", n_jobs=n_jobs)(
            joblib.delayed(process_movie_row)(row, working_proxies)
            for row in rows_to_process
        )

    result_dict = {index: imdb_id for index, imdb_id in results}
    df["IMDB movie ID"] = df["index"].map(result_dict)
    df.drop("index", axis=1, inplace=True)
    df.to_csv(output_file, index=False)


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
        "--use_parallel",
        type=bool,
        default=False,
        help="whether to use parallel processing",
    )
    parser.add_argument(
        "--n_jobs",
        type=int,
        default=-1,
        help="number of parallel jobs to run (default: -1, use all cores)",
    )

    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s: %(message)s",
        filename=os.path.join(args.output_dir, "imdb_retrieval.log"),
    )

    proxy_list = args.proxies.split(",") if args.proxies else None

    input_file = os.path.join(args.data_dir, "movie_processed.csv")
    output_file = os.path.join(args.output_dir, "movie_processed_with_imdb.csv")
    checkpoint_file = os.path.join(args.output_dir, "imdb_retrieval_checkpoint.json")

    print("Starting IMDB data retrieval...")
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"Checkpoint file: {checkpoint_file}")
    print(f"Processing {args.n_rows if args.n_rows != -1 else 'all'} rows")
    print(f"Using {args.n_jobs} parallel jobs")
    print(f"Proxy servers: {proxy_list or 'None'}")

    n_rows = None if args.n_rows == -1 else args.n_rows

    if args.use_parallel:
        add_imdb_ids_parallel(input_file, output_file, n_rows, args.n_jobs, proxy_list)
    else:
        retriever = IMDBIDRetriever(
            input_file, output_file, checkpoint_file, n_rows, proxy_list
        )
        retriever.process_imdb_ids()

    print("Retrieval complete! Check imdb_retrieval.log for details.")


if __name__ == "__main__":
    main()
