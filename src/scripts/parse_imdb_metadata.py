import pandas as pd
import numpy as np
import logging
from typing import Optional
import imdb
import difflib
import re
import argparse
import os
from tqdm import tqdm

class IMDBIDRetriever:
    def __init__(self, input_file: str, output_file: str, n_rows: int):
        """
        Initialize the IMDB ID retrieval process.
        Args:
            input_file (str): Path to the input processed movie CSV file
            output_file (str): Path to save the output CSV file with IMDB movie ID
            n_rows: number of rows to process (None for all rows, int for specific number)
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
        self.n_rows = n_rows

    def clean_movie_name(self, name: str) -> str:
        """
        Clean and standardize movie name for better matching.
        
        Args:
            name (str): Original movie name
        
        Returns:
            str: Cleaned movie name
        """
        name = re.sub(r'[^\w\s]', '', name.lower())
        name = ' '.join(name.split())
        return name

    def find_imdb_id(self, row: pd.Series) -> Optional[str]:
        """
        Find IMDB ID for a movie based on multiple matching strategies.
        
        Args:
            row (pd.Series): A row from the movie dataframe
        
        Returns:
            Optional[str]: IMDB movie ID or None
        """
        try:
            movie_name = row['Movie name']
            release_year = str(row['movie_release_date'])[:4]
            clean_name = self.clean_movie_name(movie_name)
            search_results = self.imdb_search.search_movie(movie_name)
            
            matched_movies = []
            for movie in search_results:
                search_name = self.clean_movie_name(movie['title'])
                
                movie_year = str(movie.get('year', ''))
                
                similarity_ratio = difflib.SequenceMatcher(None, clean_name, search_name).ratio()
                
                if (similarity_ratio > 0.8 and 
                    movie_year == release_year):
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
        Process and add IMDB IDs to the dataframe.
        """
        try:
            self.df['IMDB movie ID'] = self.df.apply(self.find_imdb_id, axis=1)
            imdb_id_validity = self.df['IMDB movie ID'].notna().mean()

            logging.info(f"Retrieved IMDB IDs for {imdb_id_validity * 100:.2f}% of rows")

            self.df.to_csv(self.output_file, index=False)

            logging.info(f"Saved processed data to {self.output_file}")
        
        except Exception as e:
            logging.error(f"Error processing IMDB IDs: {e}")

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
            format='%(asctime)s - %(levelname)s: %(message)s',
            filename=os.path.join(args.output_dir, "imdb_retrieval.log"),
        )

    input_file = os.path.join(args.data_dir, "movie_processed.csv")
    output_file = os.path.join(args.output_dir, "movie_processed_with_imdb.csv")
    
    print("Starting IMDB data retrieval...")
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"Processing {args.n_rows if args.n_rows != -1 else 'all'} rows")

    n_rows = None if args.n_rows == -1 else args.n_rows
    retriever = IMDBIDRetriever(input_file, output_file, n_rows)
    retriever.process_imdb_ids()
    print("Retrieval complete! Check imdb_retrieval.log for details.")

if __name__ == "__main__":
    main()