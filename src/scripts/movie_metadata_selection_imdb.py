import argparse
import logging
import multiprocessing
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
from tqdm import tqdm
from tqdm_joblib import tqdm_joblib


class IMDBMetadataSelectorForMovie:
    """
    This class implements the parse through the IMDB to extract description of movie's plot
    """

    def __init__(self):
        self.base_url = "https://www.imdb.com"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }

    def extract_movie_description(self, imdb_movie_id, movie_name):
        """
        Extract the description from movie's IMDB page
        """
        url = f"{self.base_url}/title/{imdb_movie_id}/plotsummary"
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")

                synopsis_sections = soup.find_all(
                    "div", class_=["ipc-html-content", "ipc-html-content-inner"]
                )

                synopsis_texts = []
                for section in synopsis_sections:
                    text = section.get_text(strip=True)
                    if (
                        text
                        and len(text) > 50
                        and not text.endswith("synopsis, and more...")
                    ):
                        synopsis_texts.append(text)

                if synopsis_texts:
                    return " || ".join(synopsis_texts)

                meta_desc = soup.find("meta", attrs={"name": "description"})
                if meta_desc:
                    desc = meta_desc.get("content", "")
                    if desc and not desc.endswith("synopsis, and more..."):
                        return desc

            return None

        except Exception as e:
            logging.warning(
                f"Failed to retrieve movie description for {movie_name}: {str(e)}"
            )
            return None

    def process_row(self, row):
        movie_description = self.extract_movie_description(
            row["IMDB movie ID"], row["Movie name"]
        )
        return movie_description

    def enrich_movie_data(self, input_file, output_file, n_rows=None, n_jobs=-1):
        """
        Main function to process the CSV and add movie descriptions
        """
        df = pd.read_csv(input_file)
        if n_rows:
            df = df.head(n_rows)

        num_cores = multiprocessing.cpu_count() if n_jobs == -1 else n_jobs

        with tqdm_joblib(tqdm(desc="Processing rows", total=len(df))):
            results = Parallel(n_jobs=num_cores)(
                delayed(self.process_row)(row) for _, row in df.iterrows()
            )

        df["movie_description_imdb"] = results

        df.to_csv(output_file, index=False)
        logging.info(f"Data enriched and saved to {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Extract movie descriptions from IMDB")
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
        "--n_jobs",
        type=int,
        default=-1,
        help="number of parallel jobs to run (default: -1, use all available cores)",
    )

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=os.path.join(args.output_dir, "movie_enrichment.log"),
    )

    input_file = os.path.join(args.data_dir, "movie_processed_with_imdb.csv")
    output_file = os.path.join(args.output_dir, "movie_processed_enriched_by_imdb.csv")

    print("Starting movie data enrichment...")
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"Processing {args.n_rows if args.n_rows != -1 else 'all'} rows")
    print(f"Using {args.n_jobs if args.n_jobs != -1 else 'all available'} CPU cores")

    n_rows = None if args.n_rows == -1 else args.n_rows
    selector = IMDBMetadataSelectorForMovie()
    selector.enrich_movie_data(input_file, output_file, n_rows, args.n_jobs)
    print("Enrichment complete! Check movie_enrichment.log for details.")


if __name__ == "__main__":
    main()
