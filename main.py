from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import pandas as pd
import requests
import io
import gzip
import logging
from tqdm import tqdm
import datetime

# Define the URL of the gzipped CSV file
TOP_SITES_URL = "https://raw.githubusercontent.com/zakird/crux-top-lists/main/data/global/current.csv.gz"
TOP_SITES_CACHE = "./cache-top-sites.csv.gz"

# Maximum number of sites to consider
MAX_RANK = 5000
MAX_NUM_SITES = MAX_RANK
# MAX_NUM_SITES = 50

# Keyword to search for in the homepage content
KEYWORD = "polyfill.io"
# KEYWORD = "googletagmanager.com"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] (%(levelname)s) %(message)s",
)


def main():
    logging.info("Starting the script.")
    top_sites: pd.DataFrame = get_top_sites(
        url=TOP_SITES_URL, cache_file=TOP_SITES_CACHE
    )
    logging.info(f"Loaded {len(top_sites)} top sites.")

    ranks = top_sites["rank"].unique()
    logging.info(f"Unique ranks: {ranks}")

    # Filter just the most popular sites
    max_rank_sites: pd.DataFrame = top_sites[top_sites["rank"] <= MAX_RANK]
    logging.info(f"Filtered to {len(max_rank_sites)} sites with rank <= {MAX_RANK}.")

    # Limit the number of results
    max_rank_sites = max_rank_sites.head(MAX_NUM_SITES)
    logging.info(f"Limiting to the top {MAX_NUM_SITES} sites.")

    # Fetch the home page of each site, and scan for polyfill.io
    fetch_homepages_in_parallel(max_rank_sites)
    logging.info("Finished fetching homepages.")

    # Filter out sites that have no homepage content
    max_rank_sites = max_rank_sites[max_rank_sites["homepage_content"].str.len() > 0]
    logging.info(f"Filtered to {len(max_rank_sites)} sites with homepage content.")

    # Filter sites that use our keyword
    max_rank_sites["keyword_line"] = max_rank_sites["homepage_content"].apply(
        find_keyword_context, keyword=KEYWORD
    )
    sites_with_keyword: pd.DataFrame = max_rank_sites[
        max_rank_sites["keyword_line"].notnull()
    ]
    logging.info(f"Found {len(sites_with_keyword)} sites with the keyword '{KEYWORD}'.")

    print(sites_with_keyword[["origin", "keyword_line"]])

    # Save the results to a CSV file
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
    keyword_as_filename = KEYWORD.replace(".", "_")
    filename = f"sites_with_{keyword_as_filename}_{timestamp}.csv"

    sites_with_keyword[["origin", "keyword_line"]].to_csv(filename, index=False)


def get_top_sites(url, cache_file):
    """
    Download the top sites CSV file and return it as a pandas DataFrame.

    Example dataframe:
    ```
                         origin  rank
    0  https://web.facebook.com  1000
    ```
    """

    # Check if the cache file exists
    if os.path.exists(cache_file):
        # Read the content from the cache file
        with open(cache_file, "rb") as f:
            compressed_content = f.read()
    else:
        # Send a GET request to the URL
        response = requests.get(url)
        # Ensure the request was successful
        if not response.ok:
            raise Exception(
                f"Failed to download the file. Status code: " f"{response.status_code}"
            )
        # Cache the content of the response on disk
        with open(cache_file, "wb") as f:
            f.write(response.content)
        compressed_content = response.content

    # Decompress the content
    content = gzip.decompress(compressed_content)

    # Read the content into a pandas DataFrame
    return pd.read_csv(io.BytesIO(content))


def fetch_homepages_in_parallel(max_rank_sites: pd.DataFrame):
    """
    Fetch the homepages of a list of URLs in parallel.
    """
    # Use ThreadPoolExecutor to fetch homepages in parallel
    with ThreadPoolExecutor(max_workers=20) as executor:
        # Submit all fetch tasks and create a dictionary of future to url
        future_to_url = {
            executor.submit(fetch_homepage_content, url, timeout=10): url
            for url in max_rank_sites["origin"]
        }

        # Collect results as they complete
        for future in tqdm(
            as_completed(future_to_url),
            desc="Fetching homepages",
            total=len(future_to_url),
        ):
            url = future_to_url[future]
            try:
                content = future.result()
                # Assign the result back to the DataFrame
                max_rank_sites.loc[
                    max_rank_sites["origin"] == url, "homepage_content"
                ] = content
            except Exception as e:
                logging.error(f"Error fetching content for {url}: {e}")


def fetch_homepage_content(url, timeout=None):
    """
    Fetch the content of the homepage of a given URL.
    """
    try:
        response = requests.get(url, timeout=timeout)
        return response.text
    except Exception as e:
        logging.error(f"Failed to fetch the homepage of {url}. Error: {e}")
        return ""


def find_keyword_context(text: str, keyword: str):
    for i, line in enumerate(text.splitlines()):
        line = line.lower().strip()
        keyword_index = line.find(keyword)
        if keyword_index == -1:
            continue

        context = line[max(0, keyword_index - 50) : keyword_index + 50]
        return f"{i}: {context}"

    return None


if __name__ == "__main__":
    main()
