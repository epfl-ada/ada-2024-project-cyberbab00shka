import argparse
import logging
import os
<<<<<<< HEAD
import re
import time
from typing import Dict, Optional
=======
import json
from typing import Optional, Dict
>>>>>>> main

import pandas as pd
import wikipediaapi
from tqdm import tqdm
import joblib
import time


class WikipediaMetadataSelectorForMovie:
    """
    This class implements the parse through the Wikipedia to extract the description of the movie's basic metadata
    """

    def __init__(self):
        self.wiki = wikipediaapi.Wikipedia(
            user_agent="MovieMetadataSelector", language="en"
        )
        self.page_cache = {}

    def get_wiki_page(self, title: str) -> Optional[wikipediaapi.WikipediaPage]:
        """
        Fetch Wikipedia page with error handling and caching
        """
        if title in self.page_cache:
            return self.page_cache[title]

        try:
            page = self.wiki.page(title + " (film)")
            if page.exists():
                self.page_cache[title] = page
                return page
            page = self.wiki.page(title + "(film)")
            if page.exists():
                self.page_cache[title] = page
                return page
            page = self.wiki.page(title)
            if page.exists():
                self.page_cache[title] = page
                return page
            return None
        except Exception as e:
            logging.error(f"Error fetching Wikipedia page for {title}: {str(e)}", exc_info=True)
            return None

    def extract_metadata_description(self, movie_id: str) -> Dict[str, Optional[str]]:
        """
        Extract movie's metadata description from its Wikipedia page
        """
        page = self.get_wiki_page(str(movie_id))
        if not page or not page.exists():
            return {}

        metadata = {
            "wikipedia_movie_id": movie_id,
            "release_date": None,
            "plot_summary": None,
            "genres": None,
            "keywords": None,
            "cast": None,
        }

        basic_info = page.section_by_title("Infobox")
        if basic_info:
            for line in basic_info.text.splitlines():
                if "Release dates" in line:
                    metadata["release_date"] = line.split(":")[-1].strip()
                elif "Genre" in line:
                    metadata["genres"] = line.split(":")[-1].strip()
                elif "Starring" in line:
                    metadata["cast"] = line.split(":")[-1].strip()

        cast_info = (
            page.section_by_title("Cast")
            or page.section_by_title("Reparto")
        )
        if cast_info and cast_info.text.strip():
            metadata["cast"] = cast_info.text.strip()

        plot_section = (
            page.section_by_title("Plot")
            or page.section_by_title("Plot ")
            or page.section_by_title("Story")
            or page.section_by_title("Story ")
            or page.section_by_title("Plot summary")
            or page.section_by_title("Plot summary ")
            or page.section_by_title("Synopsis")
            or page.section_by_title("Synopsis ")
        )
        if plot_section:
            metadata["plot_summary"] = plot_section.text.strip()
        else:
            metadata["plot_summary"] = page.text.strip()

        metadata["page_summary"] = page.summary.strip()
        metadata["page"] = page.text.strip()
        return metadata

def process(row: dict):
    selector = WikipediaMetadataSelectorForMovie()
    title = row['wiki_api_title']
    if not isinstance(title, str):
        title = row['Movie name']
    try:
        metadata = selector.extract_metadata_description(title)
    except:
        return row
    return row | metadata


def enrich_movie_data(input_file: str, output_file: str, n_rows: int | None = None):
    """
    Main function to process the CSV and add movie metadata descriptions
    """
    try:
        df = pd.read_csv(
            input_file,
            low_memory=False,
            dtype={
                0: str,  # wiki_movie_id as string
            },
            nrows=n_rows,
        )

        print(f"Processing {len(df)} rows...")
        print(df.columns)

        result = []
        try:
            with open(output_file) as file:
                result = json.load(file)
                already = [i["Wikipedia movie ID"] for i in result if 'page_summary' in i]
                df = df[~df["Wikipedia movie ID"].isin(already)]
        except FileNotFoundError:
            pass

        last_time = 0
        for i, row in enumerate(
                    joblib.Parallel(return_as='generator', n_jobs=16)(
                    joblib.delayed(process)(row.to_dict())
                    for _, row in tqdm(df.iterrows(), total=len(df))
                )
            ):
            result.append(row)
            if abs(time.time() - last_time) > 60:
                last_time = time.time()
                with open(output_file, 'w') as file:
                    json.dump(result, file, indent=2)

        with open(output_file, 'w') as file:
            json.dump(result, file)
        logging.info(f"Successfully saved enriched data to {output_file}")

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise


def main():
    current_dir = os.getcwd()

    parser = argparse.ArgumentParser(
        description="Extract movie metadata from Wikipedia"
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        default=os.path.join(current_dir, "data/MovieSummaries"),
        help="path to MovieSummaries",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=os.path.join(current_dir, "data/MovieSummaries"),
        help="path where to store outputs",
    )
    parser.add_argument(
        "--n_rows",
        type=int,
        default=-1,
        help="number of rows to process (default: 100, use -1 for all rows)",
    )
    parser.add_argument(
        "--input_file_name",
        type=str,
        default="movie_processed.csv",
        help="name of the input file",
    )
    parser.add_argument(
        "--output_file_name",
        type=str,
        default="movie_processed_enriched.json",
        help="name of the output file",
    )

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=os.path.join(args.output_dir, "movie_enrichment.log"),
    )

    input_file = os.path.join(args.data_dir, args.input_file_name)
    output_file = os.path.join(args.output_dir, args.output_file_name)

    print("Starting movie data enrichment...")
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"Processing {args.n_rows if args.n_rows != -1 else 'all'} rows")

    n_rows = None if args.n_rows == -1 else args.n_rows
    enrich_movie_data(input_file, output_file, n_rows)
    print("Enrichment complete! Check movie_enrichment.log for details.")


if __name__ == "__main__":
    main()
