# src/scraping/scrape_ufcstats.py

import time
from pathlib import Path
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from typing import List


BASE_URL = "http://ufcstats.com"
EVENTS_COMPLETED_URL = f"{BASE_URL}/statistics/events/completed"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}


def fetch_page(url: str, retries: int = 3, sleep_seconds: float = 3.0) -> BeautifulSoup:
    """
    Fetch a page and return a BeautifulSoup object.
    Retries a few times on network errors / timeouts.
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)  # bump timeout to 20s
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            last_err = e
            print(f"[fetch_page] Error fetching {url} (attempt {attempt}/{retries}): {e}")
            time.sleep(sleep_seconds)

    # If we get here, all retries failed
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts. Last error: {last_err}")


def scrape_all(max_pages: int = 30):
    """
    Scrape UFCStats events and fights and save to data/raw/ufc_fights_raw.csv
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Get events
    event_urls = scrape_event_urls(max_pages=max_pages)
    print(f"Total unique event URLs collected: {len(event_urls)}")

    # 2) For each event, collect fights and details
    all_fights = []

    for event_url in event_urls:
        # Use the more robust fight scraper
        fights = scrape_fights_for_event(event_url)
        print(f"  Found {len(fights)} fights for event {event_url}")

        for fight in fights:
            details = scrape_fight_details(fight["fight_url"])

            merged = {
                "event_url": event_url,  # this will just match what's already in fight["event_url"]
                **fight,
                **details,
            }
            all_fights.append(merged)

        time.sleep(0.5)  # be polite to the site

    if not all_fights:
        print("No fights scraped.")
        return

    df = pd.DataFrame(all_fights)
    out_path = DATA_DIR / "ufc_fights_raw.csv"
    df.to_csv(out_path, index=False)

    print(f"Saved {len(df)} fights to {out_path}")


def build_events_page_url(page: int) -> str:
    """
    Build the URL for the completed events page.

    page = 0  -> latest events (default view)
    page = 1  -> 'all' events on one page
    For page > 1, we just keep returning 'all' as well.
    """
    if page == 0:
        return EVENTS_COMPLETED_URL  # latest events
    else:
        # one big page with the full event history
        return f"{EVENTS_COMPLETED_URL}?page=all"


def scrape_event_urls(max_pages: int = 2) -> List[str]:
    """
    Scrape UFCStats completed event pages and collect event-detail URLs.

    max_pages: how many pages of completed events to scrape.
               Start small (2–3) while testing.
    """
    all_event_urls: List[str] = []
    seen: set[str] = set()

    for page in range(max_pages):
        page_url = build_events_page_url(page)
        print(f"Fetching events page {page}: {page_url}")

        try:
            soup = fetch_page(page_url)
        except Exception as e:
            print(f"Error fetching {page_url}: {e}")
            break

        # Find all <a> tags with an href that contains '/event-details/'
        page_event_urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]

            if "/event-details/" in href:
                # Make absolute URL if it's relative
                if href.startswith("http"):
                    event_url = href
                else:
                    event_url = BASE_URL.rstrip("/") + "/" + href.lstrip("/")

                if event_url not in seen:
                    seen.add(event_url)
                    all_event_urls.append(event_url)
                    page_event_urls.append(event_url)

        print(f"  Found {len(page_event_urls)} events on page {page}")

        # If no events were found on this page, assume we're done
        if len(page_event_urls) == 0:
            print("  No more events found, stopping pagination.")
            break

        # Be polite: brief delay between page requests
        time.sleep(1.0)

    print(f"Total unique event URLs collected: {len(all_event_urls)}")
    return all_event_urls


def scrape_fights_for_event(event_url: str):
    """
    Scrape all fights from a single event page.
    Returns a list of dicts, each representing one fight.
    """
    print(f"  Scraping fights for event: {event_url}")

    try:
        soup = fetch_page(event_url)
    except Exception as e:
        print(f"  Error fetching event page {event_url}: {e}")
        return []
    
    # Extract event date (found in the page header)
    event_date = None
    date_tag = soup.find("li", class_="b-list__box-list-item")
    if date_tag:
        text = date_tag.get_text(" ", strip=True)
        # Example: "Date: March 2, 2024"
        if "Date:" in text:
            event_date = text.split("Date:", 1)[1].strip()

    fights = []

    # 1) Find the main fight table
    fight_table = soup.find("table", class_="b-fight-details__table")
    if not fight_table:
        print(f"  Could not find fight table for event {event_url}")
        return []

    tbody = fight_table.find("tbody")
    if not tbody:
        print(f"  Fight table has no <tbody> for event {event_url}")
        return []

    # 2) Each row in tbody is a fight
    rows = tbody.find_all("tr", class_="b-fight-details__table-row")
    print(f"  Found {len(rows)} fight rows in table")

    for row in rows:
        cols = row.find_all("td", class_="b-fight-details__table-col")
        if len(cols) < 7:
            continue

        # Fighter names are usually both in the 2nd column (index 1)
        name_paras = cols[1].find_all("p")
        if len(name_paras) >= 2:
            red_fighter = name_paras[0].get_text(strip=True)
            blue_fighter = name_paras[1].get_text(strip=True)
        else:
            # Fallback: if structure is different
            red_fighter = cols[1].get_text(strip=True)
            blue_fighter = ""  # we can refine later if needed

        weight_class = cols[6].get_text(strip=True)

        # Fight detail URL is in the row's data-link attribute
        fight_url = row.get("data-link")
        if not fight_url:
            # Fallback: try to find an <a> in the first column
            fight_link_tag = cols[0].find("a")
            if fight_link_tag and fight_link_tag.has_attr("href"):
                fight_url = fight_link_tag["href"]

        if not fight_url:
            continue

        fights.append({
            "event_url": event_url,
            "fight_url": fight_url,
            "red_fighter": red_fighter,
            "blue_fighter": blue_fighter,
            "weight_class": weight_class,
            "event_date": event_date,
        })

    return fights


def parse_made_of(value: str):
    """
    Helper to parse strings like '20 of 45' into (20, 45).
    Returns (None, None) if parsing fails.
    """
    if not value:
        return None, None
    parts = value.split("of")
    if len(parts) != 2:
        return None, None
    try:
        made = int(parts[0].strip())
        attempted = int(parts[1].strip())
        return made, attempted
    except ValueError:
        return None, None


def parse_height_to_inches(text: str):
    """
    Convert height like "5' 11\"" or "5'11\"" into inches (float).
    Returns None if parsing fails or text is missing.
    """
    if not isinstance(text, str):
        return None
    text = text.strip()
    if text in ("--", ""):
        return None

    # Look for feet' inches"
    m = re.match(r"(\d+)\s*'\s*(\d+)\s*\"", text)
    if not m:
        # Try without the closing quote
        m = re.match(r"(\d+)\s*'\s*(\d+)", text)
    if not m:
        return None

    feet = int(m.group(1))
    inches = int(m.group(2))
    return feet * 12 + inches


def parse_reach_to_inches(text: str):
    """
    Convert reach like '72"' or '72.0"' into inches (float).
    Returns None if parsing fails.
    """
    if not isinstance(text, str):
        return None
    text = text.strip()
    if text in ("--", ""):
        return None

    # Strip the trailing quote
    text = text.replace('"', "")
    try:
        return float(text)
    except ValueError:
        return None


def parse_float_stat(text: str):
    """
    Parse a generic numeric stat from text like '4.22', '52%', '--'.
    Strips '%' and returns float or None.
    """
    if not isinstance(text, str):
        return None
    text = text.strip()
    if text in ("--", ""):
        return None

    text = text.replace("%", "").strip()
    try:
        return float(text)
    except ValueError:
        return None


def scrape_fighter_profile(fighter_url: str) -> dict:
    """
    Scrape a single fighter's profile page on UFCStats.

    Returns a dict with fields like:
    - fighter_url
    - fighter_name
    - height_in
    - reach_in
    - stance
    - dob
    - slpm
    - sapm
    - str_acc
    - str_def
    - td_avg
    - td_acc
    - td_def
    - sub_avg
    """
    try:
        soup = fetch_page(fighter_url)
    except Exception as e:
        print(f"Error fetching fighter page {fighter_url}: {e}")
        return {
            "fighter_url": fighter_url,
            "fighter_name": None,
            "height_in": None,
            "reach_in": None,
            "stance": None,
            "dob": None,
            "slpm": None,
            "sapm": None,
            "str_acc": None,
            "str_def": None,
            "td_avg": None,
            "td_acc": None,
            "td_def": None,
            "sub_avg": None,
        }

    # Initialize result
    result = {
        "fighter_url": fighter_url,
        "fighter_name": None,
        "height_in": None,
        "reach_in": None,
        "stance": None,
        "dob": None,
        "slpm": None,
        "sapm": None,
        "str_acc": None,
        "str_def": None,
        "td_avg": None,
        "td_acc": None,
        "td_def": None,
        "sub_avg": None,
    }

    # 1) Fighter name (usually at the top)
    name_tag = soup.find("span", class_="b-content__title-highlight")
    if name_tag:
        result["fighter_name"] = name_tag.get_text(strip=True)

    # 2) Info boxes (height, reach, stance, DOB, etc.)
    info_boxes = soup.find_all("div", class_="b-list__info-box")
    for box in info_boxes:
        for li in box.find_all("li", class_="b-list__box-list-item"):
            text = " ".join(li.get_text(" ", strip=True).split())
            # Example text patterns:
            # "Height: 5' 11\""
            # "Reach: 72\""
            # "STANCE: Orthodox"
            # "DOB: 1995-02-10"
            if ":" not in text:
                continue
            label, value = [part.strip() for part in text.split(":", 1)]
            label_upper = label.upper()

            if label_upper == "HEIGHT":
                result["height_in"] = parse_height_to_inches(value)
            elif label_upper == "REACH":
                result["reach_in"] = parse_reach_to_inches(value)
            elif label_upper == "STANCE":
                result["stance"] = value if value not in ("--", "") else None
            elif label_upper == "DOB":
                result["dob"] = value if value not in ("--", "") else None

    # 3) Career stats (SLpM, SApM, STR. ACC, STR. DEF, TD AVG, TD ACC, TD DEF, SUB. AVG)
    # These are usually in some stats box; we just scan all list items and match labels.
    stats_labels_map = {
        "SLPM": "slpm",
        "SAPM": "sapm",
        "STR. ACC.": "str_acc",
        "STR. DEF.": "str_def",
        "TD AVG.": "td_avg",
        "TD ACC.": "td_acc",
        "TD DEF.": "td_def",
        "SUB. AVG.": "sub_avg",
    }

    for box in info_boxes:
        for li in box.find_all("li", class_="b-list__box-list-item"):
            text = " ".join(li.get_text(" ", strip=True).split())
            if ":" not in text:
                continue
            label, value = [part.strip() for part in text.split(":", 1)]
            label_upper = label.upper()

            for key, field_name in stats_labels_map.items():
                if key in label_upper:
                    result[field_name] = parse_float_stat(value)

    return result


def scrape_all_fighter_profiles(limit: int = None):
    """
    Loop over fighters_index.csv and scrape profiles for each fighter.
    Saves results to data/raw/fighter_profiles_raw.csv

    limit: if provided, only scrape the first N fighters (useful for testing).
    """
    fighters_csv = DATA_DIR / "fighters_index.csv"
    if not fighters_csv.exists():
        print(f"No fighters_index.csv found at {fighters_csv}. Run build_fighter_index_from_fights() first.")
        return

    fighters_df = pd.read_csv(fighters_csv)
    print(f"Loaded {len(fighters_df)} fighters from {fighters_csv}")

    if limit is not None:
        fighters_df = fighters_df.head(limit)
        print(f"Limiting to first {limit} fighters for this run")

    profiles = []

    for _, row in tqdm(fighters_df.iterrows(), total=len(fighters_df), desc="Scraping fighter profiles"):
        f_name = row["fighter_name"]
        f_url = row["fighter_url"]

        profile = scrape_fighter_profile(f_url)

        # Make sure we keep the name from the index as well
        profile["fighter_name_index"] = f_name

        profiles.append(profile)

        # Be polite to the site
        time.sleep(0.4)

    if not profiles:
        print("No fighter profiles scraped.")
        return

    profiles_df = pd.DataFrame(profiles)

    out_path = DATA_DIR / "fighter_profiles_raw.csv"
    profiles_df.to_csv(out_path, index=False)

    print(f"Saved {len(profiles_df)} fighter profiles to {out_path}")


def scrape_fight_details(fight_url: str) -> dict:
    """
    Scrape detailed stats for a single fight.
    Returns a dict with stats for red and blue fighters.
    """
    try:
        soup = fetch_page(fight_url)
    except Exception as e:
        print(f"    Error fetching fight page {fight_url}: {e}")
        return {}

    result = {
        "fight_url": fight_url,
        "winner": None,
        "red_kd": None,
        "blue_kd": None,
        "red_sig_str_landed": None,
        "red_sig_str_attempted": None,
        "blue_sig_str_landed": None,
        "blue_sig_str_attempted": None,
        "red_td_landed": None,
        "red_td_attempted": None,
        "blue_td_landed": None,
        "blue_td_attempted": None
    }

    ########################################
    #   1) Winner parsing
    ########################################
    fighter_blocks = soup.find_all("div", class_="b-fight-details__person")
    if len(fighter_blocks) >= 2:
        red_status = fighter_blocks[0].find("i", class_="b-fight-details__person-status")
        blue_status = fighter_blocks[1].find("i", class_="b-fight-details__person-status")

        red_text = red_status.get_text(strip=True) if red_status else ""
        blue_text = blue_status.get_text(strip=True) if blue_status else ""

        if "W" in red_text:
            result["winner"] = "Red"
        elif "W" in blue_text:
            result["winner"] = "Blue"

    ########################################
    #   2) Locate the correct totals table
    ########################################
    totals_table = None

    for table in soup.find_all("table", class_="b-fight-details__table"):
        thead = table.find("thead")
        if not thead:
            continue

        header_text = thead.get_text(" ", strip=True).lower()

        # Must match your columns exactly
        if "fighter" in header_text and "kd" in header_text and "sig. str." in header_text:
            totals_table = table
            break

    if not totals_table:
        # We failed to detect the correct table
        return result

    tbody = totals_table.find("tbody")
    if not tbody:
        return result

    row = tbody.find("tr", class_="b-fight-details__table-row")
    if not row:
        return result

    cells = row.find_all("td", class_="b-fight-details__table-col")
    # Expected order:
    # [fighter names, KD, Sig Str, Sig Str %, Total Str, TD, TD %, Sub, Rev, Ctrl]
    if len(cells) < 6:
        return result

    ########################################
    #   3) Extract stats from <p> pairs
    ########################################

    # KD
    kd_ps = cells[1].find_all("p")
    if len(kd_ps) >= 2:
        try:
            result["red_kd"] = int(kd_ps[0].get_text(strip=True) or 0)
            result["blue_kd"] = int(kd_ps[1].get_text(strip=True) or 0)
        except:
            pass

    # SIG STR ("X of Y")
    sig_ps = cells[2].find_all("p")
    if len(sig_ps) >= 2:
        rs_made, rs_att = parse_made_of(sig_ps[0].get_text(strip=True))
        bs_made, bs_att = parse_made_of(sig_ps[1].get_text(strip=True))
        result["red_sig_str_landed"] = rs_made
        result["red_sig_str_attempted"] = rs_att
        result["blue_sig_str_landed"] = bs_made
        result["blue_sig_str_attempted"] = bs_att

    # TD ("A of B")
    td_ps = cells[5].find_all("p")
    if len(td_ps) >= 2:
        rt_made, rt_att = parse_made_of(td_ps[0].get_text(strip=True))
        bt_made, bt_att = parse_made_of(td_ps[1].get_text(strip=True))
        result["red_td_landed"] = rt_made
        result["red_td_attempted"] = rt_att
        result["blue_td_landed"] = bt_made
        result["blue_td_attempted"] = bt_att

    return result


def extract_fighter_urls_from_fight(fight_url: str) -> dict:
    """
    Given a fight_details URL, return the fighter names + profile URLs.

    Returns a dict like:
    {
        "fight_url": ...,
        "red_fighter_name": ...,
        "red_fighter_url": ...,
        "blue_fighter_name": ...,
        "blue_fighter_url": ...
    }
    """
    try:
        soup = fetch_page(fight_url)
    except Exception as e:
        print(f"Error fetching fight page for fighter URLs ({fight_url}): {e}")
        return {
            "fight_url": fight_url,
            "red_fighter_name": None,
            "red_fighter_url": None,
            "blue_fighter_name": None,
            "blue_fighter_url": None,
        }

    # On the fight details page, fighters appear in "b-fight-details__person" blocks
    fighter_blocks = soup.find_all("div", class_="b-fight-details__person")

    red_name = blue_name = red_url = blue_url = None

    if len(fighter_blocks) >= 2:
        # Red corner
        red_block = fighter_blocks[0]
        red_name_tag = red_block.find("a", class_="b-link")
        if red_name_tag:
            red_name = red_name_tag.get_text(strip=True)
            red_url = red_name_tag.get("href")

        # Blue corner
        blue_block = fighter_blocks[1]
        blue_name_tag = blue_block.find("a", class_="b-link")
        if blue_name_tag:
            blue_name = blue_name_tag.get_text(strip=True)
            blue_url = blue_name_tag.get("href")

    return {
        "fight_url": fight_url,
        "red_fighter_name": red_name,
        "red_fighter_url": red_url,
        "blue_fighter_name": blue_name,
        "blue_fighter_url": blue_url,
    }

def build_fighter_index_from_fights():
    """
    Reads ufc_fights_raw.csv and builds an index of unique fighters
    with their UFCStats profile URLs. Saves to data/raw/fighters_index.csv
    """
    fights_csv = DATA_DIR / "ufc_fights_raw.csv"
    if not fights_csv.exists():
        print(f"Could not find {fights_csv}. Run scrape_all() first.")
        return

    print(f"Loading fights from {fights_csv}...")
    fights_df = pd.read_csv(fights_csv)

    unique_fight_urls = fights_df["fight_url"].dropna().unique()
    print(f"Found {len(unique_fight_urls)} unique fight URLs.")

    fighter_rows = []

    for fight_url in tqdm(unique_fight_urls, desc="Fights (for fighter index)"):
        info = extract_fighter_urls_from_fight(fight_url)

        # Red fighter
        if info.get("red_fighter_url"):
            fighter_rows.append({
                "fighter_name": info.get("red_fighter_name"),
                "fighter_url": info.get("red_fighter_url"),
            })

        # Blue fighter
        if info.get("blue_fighter_url"):
            fighter_rows.append({
                "fighter_name": info.get("blue_fighter_name"),
                "fighter_url": info.get("blue_fighter_url"),
            })

        # Be polite to the site
        time.sleep(0.3)

    if not fighter_rows:
        print("No fighter rows collected.")
        return

    fighters_df = pd.DataFrame(fighter_rows)

    # Drop duplicates by fighter_url, keep first name we saw
    fighters_df = fighters_df.dropna(subset=["fighter_url"]).drop_duplicates("fighter_url")

    out_path = DATA_DIR / "fighters_index.csv"
    fighters_df.to_csv(out_path, index=False)

    print(f"Saved {len(fighters_df)} unique fighters to {out_path}")


if __name__ == "__main__":
    # Step A: rescrape all fights with more events
    scrape_all(max_pages=2) # adjust max_pages to more if you want more data 2 page = ~750 fights







