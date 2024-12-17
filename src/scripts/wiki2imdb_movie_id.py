import argparse
import logging
import os

import imdb
import pandas as pd
from tqdm import tqdm


def add_imdb_movie_id(character_input_file, movie_input_file, output_dir, n_rows: int):
    """
    Add IMDB Movie ID to character processed CSV by matching Wikipedia Movie ID
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    try:
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

        columns = list(df_characters.columns)
        imdb_id_index = columns.index("Freebase movie ID") + 1
        columns.insert(imdb_id_index, columns.pop(columns.index("IMDB movie ID")))
        df_characters = df_characters[columns]

        df_characters.to_csv(output_dir, index=False)

        total_rows = len(df_characters)
        matched_rows = df_characters["IMDB movie ID"].notna().sum()
        logging.info(f"Total rows: {total_rows}")
        logging.info(f"Rows with matched IMDB Movie ID: {matched_rows}")
        logging.info(f"Matching percentage: {matched_rows/total_rows*100:.2f}%")

        print(f"Successfully added IMDB Movie IDs. Output saved to {output_dir}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise


def add_imdb_identifiers(
    character_input_file, movie_input_file, output_dir, n_rows: int
):
    """
    Add IMDB Movie ID, Actor ID, and Character ID to character processed CSV using IMDB API
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    try:
        imdb_search = imdb.IMDb()

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

        progress_bar = tqdm(
            df_characters.iterrows(),
            total=len(df_characters),
            desc="Processing Rows",
            unit="row",
        )

        for idx, row in progress_bar:
            if pd.isna(row["IMDB movie ID"]):
                progress_bar.set_postfix(status="No Movie ID")
                continue

            try:
                actor_search_results = imdb_search.search_person(row["Actor name"])
                if actor_search_results:
                    actor = actor_search_results[0]
                    imdb_search.update(actor)
                    df_characters.at[idx, "IMDB actor ID"] = f"nm{actor.personID}"

                movie = imdb_search.get_movie(row["IMDB movie ID"].replace("tt", ""))

                for cast_member in movie.get("cast", []):
                    if row["Actor name"].lower() in cast_member.get("name", "").lower():
                        character_name = cast_member.currentRole
                        if (
                            character_name
                            and row["Character name"].lower()
                            in str(character_name).lower()
                        ):
                            df_characters.at[
                                idx, "IMDB character ID"
                            ] = f"ch{hash(str(character_name)) % 10000}"
                            break

                progress_bar.set_postfix(status="Processed")

            except Exception as e:
                logging.warning(f"Error processing row {idx}: {str(e)}")
                continue

        progress_bar.close()

        columns = list(df_characters.columns)
        imdb_id_index = columns.index("Freebase movie ID") + 1

        columns.insert(imdb_id_index, columns.pop(columns.index("IMDB movie ID")))

        columns.insert(imdb_id_index + 1, columns.pop(columns.index("IMDB actor ID")))
        columns.insert(
            imdb_id_index + 2, columns.pop(columns.index("IMDB character ID"))
        )

        df_characters = df_characters[columns]

        df_characters.to_csv(output_dir, index=False)

        total_rows = len(df_characters)
        matched_movie_rows = df_characters["IMDB movie ID"].notna().sum()
        matched_actor_rows = df_characters["IMDB actor ID"].notna().sum()
        matched_character_rows = df_characters["IMDB character ID"].notna().sum()

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

    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    character_input_file = os.path.join(args.data_dir, "character_processed.csv")
    movie_input_file = os.path.join(args.data_dir, "movie_processed_with_imdb.csv")
    output_file = os.path.join(args.output_dir, "character_processed_with_imdb.csv")

    print(f"Character file: {character_input_file}")
    print(f"Movie file: {movie_input_file}")
    print(f"Output file: {output_file}")
    print(f"Processing {args.n_rows if args.n_rows != -1 else 'all'} rows")

    n_rows = None if args.n_rows == -1 else args.n_rows
    # add_imdb_movie_id(character_input_file, movie_input_file, output_file, n_rows)
    add_imdb_identifiers(character_input_file, movie_input_file, output_file, n_rows)


if __name__ == "__main__":
    main()
