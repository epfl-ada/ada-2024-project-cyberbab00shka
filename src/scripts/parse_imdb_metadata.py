import argparse
import difflib
import json
import logging
import os
import re
from typing import Optional

import imdb
import numpy as np
import pandas as pd
from tqdm import tqdm


class IMDBIDRetriever:
    def __init__(
        self, input_file: str, output_file: str, checkpoint_file: str, n_rows: int
    ):
        """
        Initialize the IMDB ID retrieval process with checkpoint support
        """

        self.imdb_search = imdb.IMDb()

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
                self.df.loc[
                    self.df["Movie name"] == movie_name, "IMDB movie ID"
                ] = imdb_id

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

    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s: %(message)s",
        filename=os.path.join(args.output_dir, "imdb_retrieval.log"),
    )

    input_file = os.path.join(args.data_dir, "movie_processed.csv")
    output_file = os.path.join(args.output_dir, "movie_processed_with_imdb.csv")
    checkpoint_file = os.path.join(args.output_dir, "imdb_retrieval_checkpoint.json")

    print("Starting IMDB data retrieval...")
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"Checkpoint file: {checkpoint_file}")
    print(f"Processing {args.n_rows if args.n_rows != -1 else 'all'} rows")

    n_rows = None if args.n_rows == -1 else args.n_rows
    retriever = IMDBIDRetriever(input_file, output_file, checkpoint_file, n_rows)
    retriever.process_imdb_ids()
    print("Retrieval complete! Check imdb_retrieval.log for details.")


if __name__ == "__main__":
    main()
