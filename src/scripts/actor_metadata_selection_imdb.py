import argparse
import logging
import multiprocessing
import os
import random
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
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


def get_working_proxies(proxy_list, max_proxies=10):
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


class IMDBMetadataSelectorForActor:
    """
    This class implements parsing through IMDB to extract descriptions of characters and actors
    """

    def __init__(self, proxies=None):
        self.base_url = "https://www.imdb.com"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        self.working_proxies = get_working_proxies(proxies) if proxies else []
        logging.info(f"Initialized with {len(self.working_proxies)} working proxies")

    def _get_with_proxy(self, url):
        """
        Make a GET request with optional proxy support

        Args:
            url (str): URL to request

        Returns:
            requests.Response: Response object
        """
        try:
            if self.working_proxies:
                proxy = random.choice(self.working_proxies)
                proxies = {"http": f"http://{proxy}", "https": f"https://{proxy}"}
                return requests.get(
                    url, headers=self.headers, proxies=proxies, timeout=10
                )
            else:
                return requests.get(url, headers=self.headers, timeout=10)
        except requests.RequestException as e:
            logging.warning(f"Request failed with proxy/without proxy: {str(e)}")
            return None

    def extract_character_description(
        self, imdb_movie_id, imdb_character_id, character_name
    ):
        """
        Extract character description from movie's IMDB page
        """
        url = f"{self.base_url}/title/{imdb_movie_id}/fullcredits"
        try:
            response = self._get_with_proxy(url)

            if response and response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")

                character_link = soup.find("a", string=character_name)

                if character_link:
                    character_url = self.base_url + character_link.get("href")

                    time.sleep(random.uniform(0.5, 1.5))

                    char_response = self._get_with_proxy(character_url)

                    if char_response and char_response.status_code == 200:
                        char_soup = BeautifulSoup(char_response.content, "html.parser")

                        quotes_section = char_soup.find(
                            "div", class_="soda", id="quotes"
                        )

                        if quotes_section:
                            quotes = quotes_section.find_all("div", class_="quote")

                            quote_texts = []
                            for quote in quotes[:2]:
                                quote_text = quote.get_text(strip=True)
                                quote_texts.append(quote_text)

                            return " || ".join(quote_texts) if quote_texts else None

            return None

        except Exception as e:
            logging.warning(
                f"Failed to retrieve character description for {character_name}: {str(e)}"
            )
            return None

    def extract_actor_description(self, imdb_actor_id):
        """
        Extract actor description from their IMDB page
        """
        url = f"{self.base_url}/name/{imdb_actor_id}/"
        try:
            time.sleep(random.uniform(0.5, 1.5))

            response = self._get_with_proxy(url)

            if response and response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")

                meta_descriptions = soup.find_all("meta", attrs={"name": "description"})

                contents = []
                for meta in meta_descriptions:
                    content = meta.get("content")
                    if content:
                        contents.append(content)

                return " ".join(contents) if contents else None
            else:
                logging.warning(
                    f"Failed to retrieve actor description for {imdb_actor_id}: Status code {response.status_code if response else 'No Response'}"
                )
                return None
        except Exception as e:
            logging.warning(
                f"Failed to retrieve actor description for {imdb_actor_id}: {str(e)}"
            )
            return None

    def process_row(self, row):
        character_description = self.extract_character_description(
            row["IMDB movie ID"], row["IMDB character ID"], row["Character name"]
        )
        actor_description = self.extract_actor_description(row["IMDB actor ID"])

        return character_description, actor_description

    def enrich_character_data(self, input_file, output_file, n_rows=None, n_jobs=-1):
        """
        Main function to process the CSV and add character and actor descriptions
        """
        df = pd.read_csv(input_file)
        if n_rows:
            df = df.head(n_rows)

        num_cores = multiprocessing.cpu_count() if n_jobs == -1 else n_jobs

        with tqdm_joblib(tqdm(desc="Processing rows", total=len(df))):
            results = Parallel(n_jobs=num_cores)(
                delayed(self.process_row)(row) for _, row in df.iterrows()
            )

        df["character_description_imdb"], df["actor_description_imdb"] = zip(*results)

        df.to_csv(output_file, index=False)
        logging.info(f"Data enriched and saved to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Extract character and actor descriptions from IMDB"
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
        "--proxies",
        type=str,
        help="comma-separated list of proxy servers (ip:port)",
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
        filename=os.path.join(args.output_dir, "character_enrichment.log"),
    )

    proxy_list = args.proxies.split(",") if args.proxies else None

    input_file = os.path.join(args.data_dir, "character_processed_with_imdb.csv")
    output_file = os.path.join(
        args.output_dir, "character_processed_enriched_by_imdb.csv"
    )

    print("Starting character data enrichment...")
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    print(f"Processing {args.n_rows if args.n_rows != -1 else 'all'} rows")
    print(f"Using {args.n_jobs if args.n_jobs != -1 else 'all available'} CPU cores")
    print(f"Proxy servers: {proxy_list or 'None'}")

    n_rows = None if args.n_rows == -1 else args.n_rows
    selector = IMDBMetadataSelectorForActor(proxies=proxy_list)
    selector.enrich_character_data(input_file, output_file, n_rows, args.n_jobs)
    print("Enrichment complete! Check character_enrichment.log for details.")


if __name__ == "__main__":
    main()
