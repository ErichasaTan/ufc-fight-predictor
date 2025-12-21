# src/scraping/scrape_odds.py

import time
from pathlib import Path
import re

import requests
from bs4 import BeautifulSoup
import pandas as pd

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}


def fetch_page(url: str, timeout: float = 15.0) -> BeautifulSoup:
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def parse_american_odds(text: str):
    """
    Convert a string like '-150' or '+130' to an int.
    Returns None if parsing fails.
    """
    if text is None:
        return None
    text = text.strip()
    if not text:
        return None

    # Accept formats like -150, +130, 130, etc.
    m = re.match(r"^([+-]?\d+)", text)
    if not m:
        return None

    try:
        return int(m.group(1))
    except ValueError:
        return None


def scrape_event_odds(event_odds_url: str):
    """
    Scrape odds for a SINGLE UFC event card from an odds site.

    Returns a list of dicts:
    [
      {
        "red_fighter": "...",
        "blue_fighter": "...",
        "red_odds": -150,
        "blue_odds": +130,
        "event_url": event_odds_url,   # or a cleaned event identifier
      },
      ...
    ]

    You MUST adjust the table / selector logic to match
    the odds site you decide to use.
    """
    soup = fetch_page(event_odds_url)

    fights = []

    # --------------------------------------------------
    # TODO: Adjust selectors for your chosen odds site
    # --------------------------------------------------
    # This is just a skeleton. You need to inspect the
    # HTML of your odds site (e.g. via DevTools) and
    # update the "find(...)" lines.
    #
    # Example idea:
    #   - Find the main table that lists all fights
    #   - For each row:
    #       col 0: red fighter name
    #       col 1: blue fighter name
    #       col 2: red odds (American)
    #       col 3: blue odds (American)
    # --------------------------------------------------

    odds_table = soup.find("table")  # <- replace with more specific selector

    if not odds_table:
        print("Could not find odds table on page.")
        return fights

    rows = odds_table.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        # You will probably need to tweak this number
        if len(cols) < 4:
            continue

        red_name = cols[0].get_text(strip=True)
        blue_name = cols[1].get_text(strip=True)

        red_odds_text = cols[2].get_text(strip=True)
        blue_odds_text = cols[3].get_text(strip=True)

        red_odds = parse_american_odds(red_odds_text)
        blue_odds = parse_american_odds(blue_odds_text)

        if not red_name or not blue_name:
            continue

        # It's okay if odds are None for some fights (they might not be posted yet)
        fights.append({
            "event_odds_url": event_odds_url,
            "red_fighter": red_name,
            "blue_fighter": blue_name,
            "red_odds": red_odds,
            "blue_odds": blue_odds,
        })

    return fights


def save_event_odds_to_csv(event_odds_url: str, out_path: Path = None):
    fights = scrape_event_odds(event_odds_url)

    if not fights:
        print("No fights scraped from odds page.")
        return

    df = pd.DataFrame(fights)

    if out_path is None:
        project_root = Path(__file__).resolve().parents[2]
        out_path = project_root / "data" / "raw" / "fight_odds_latest.csv"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print(f"Saved {len(df)} fights with odds to {out_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "event_odds_url",
        help="URL of the odds page for a single UFC event (e.g., a sportsbook or odds aggregator page)."
    )
    args = parser.parse_args()

    save_event_odds_to_csv(args.event_odds_url)
