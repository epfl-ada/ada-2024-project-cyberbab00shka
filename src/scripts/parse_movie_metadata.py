import wikipediaapi

wiki = wikipediaapi.Wikipedia("en")


def get_movie_data(wikipedia_movie_id):
    page = wiki.page(wikipedia_movie_id)

    if not page.exists():
        return None

    data = {
        "wikipedia_movie_id": wikipedia_movie_id,
        "release_date": None,
        "plot_summary": None,
        "genres": None,
        "keywords": None,
        "cast": None,
    }

    for section in page.sections:
        if "release" in section.title.lower():
            data["release_date"] = section.text

        if "plot" in section.title.lower():
            data["plot_summary"] = section.text

        if "genre" in section.title.lower():
            data["genres"] = section.text

        if "keywords" in section.title.lower():
            data["keywords"] = section.text

        if "cast" in section.title.lower():
            data["cast"] = section.text

    return data
