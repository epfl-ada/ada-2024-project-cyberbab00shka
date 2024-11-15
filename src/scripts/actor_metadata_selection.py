import argparse
import logging
import os
import re
import time
from typing import Optional

import pandas as pd
import wikipediaapi
from tqdm import tqdm


class WikipediaMetadataSelectorForActor:
    """
    This class implements the parse through the wikipedia to extract description of character's and their actors.
    """

    def __init__(self):
        self.wiki = wikipediaapi.Wikipedia(
            user_agent="MovieCharacterSelector", language="en"
        )
        self.page_cache = {}

    def get_wiki_page(self, title: str) -> Optional[wikipediaapi.WikipediaPage]:
        """
        Fetch Wikipedia page with error handling and caching
        """
        if title in self.page_cache:
            return self.page_cache[title]

        try:
            page = self.wiki.page(title)
            if page.exists():
                self.page_cache[title] = page
                return page
            return None
        except Exception as e:
            logging.error(f"Error fetching Wikipedia page for {title}: {str(e)}")
            return None

    def extract_character_description(self, movie_id: str, character_name: str) -> str:
        """
        Extract character description from movie's Wikipedia page
        """
        page = self.get_wiki_page(str(movie_id))
        if not page:
            return ""

        # character mentions in the plot section
        plot_section = page.section_by_title("Plot")
        if not plot_section:
            return ""

        # find sentences containing character name
        sentences = re.split(r"(?<=[.!?])\s+", plot_section.text)
        character_sentences = [
            sent for sent in sentences if character_name.lower() in sent.lower()
        ]

        if character_sentences:
            # return the first mention of the character with some context
            return character_sentences[0].strip()
        return ""

    def extract_actor_description(self, actor_name: str) -> str:
        """
        Extract actor description from their Wikipedia page
        """
        page = self.get_wiki_page(actor_name)
        if not page:
            return ""

        # get the first paragraph of the page
        paragraphs = page.text.split("\n")
        relevant_paragraphs = [p for p in paragraphs if p.strip() and len(p) > 50]

        if relevant_paragraphs:
            # get first significant paragraph and truncate it
            description = relevant_paragraphs[0].strip()
            return description[:200] + "..." if len(description) > 200 else description
        return ""


def enrich_character_data(input_file: str, output_file: str, n_rows: int = None):
    """
    Main function to process the CSV and add character and actor descriptions
    """
    try:
        # read the CSV file
        df = pd.read_csv(
            input_file,
            header=None,
            low_memory=False,
            dtype={
                0: str,  # wiki_movie_id as string
                4: str,  # actor_height as string
            },
            nrows=n_rows,
        )

        print(f"Processing {len(df)} rows...")

        df.columns = [
            "wiki_movie_id",
            "freebase_movie_id",
            "character_name",
            "actor_gender",
            "actor_height",
            "actor_ethnicity_id",
            "actor_name",
            "freebase_map_id",
            "freebase_character_id",
            "freebase_actor_id",
            "actor_dob",
            "movie_release_date",
            "ethn_name",
            "race",
        ]

        selector = WikipediaMetadataSelectorForActor()

        # new columns for the character_processed.csv
        df["character_description"] = ""
        df["actor_description"] = ""

        for idx, row in tqdm(df.iterrows(), total=len(df)):
            time.sleep(1)

            try:
                char_desc = selector.extract_character_description(
                    row["wiki_movie_id"], row["character_name"]
                )
                actor_desc = selector.extract_actor_description(row["actor_name"])

                df.at[idx, "character_description"] = char_desc
                df.at[idx, "actor_description"] = actor_desc

                logging.info(f"Processed row {idx}: {row['character_name']}")

            except Exception as e:
                logging.error(f"Error processing row {idx}: {str(e)}")
                continue

        df.to_csv(output_file, index=False)
        logging.info(f"Successfully saved enriched data to {output_file}")

    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise


def main():

    parser = argparse.ArgumentParser(
        description="Extract character and actor descriptions from Wikipedia"
    )
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
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=os.path.join(args.output_dir, "character_enrichment.log"),
    )

    input_file = os.path.join(args.data_dir, "character_processed.csv")
    output_file = os.path.join(args.output_dir, "character_processed_enriched.csv")

    print("Starting character data enrichment...")
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"Processing {args.n_rows if args.n_rows != -1 else 'all'} rows")

    n_rows = None if args.n_rows == -1 else args.n_rows
    enrich_character_data(input_file, output_file, n_rows)
    print("Enrichment complete! Check character_enrichment.log for details.")


if __name__ == "__main__":
    main()
