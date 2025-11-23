# src/scraping/scrape_ufcstats.py

import time
from pathlib import Path

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


def fetch_page(url: str) -> BeautifulSoup:
    """Fetch a page and return a BeautifulSoup object."""
    resp = requests.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def scrape_all():
    print("Starting UFCStats scraping...")

    # 1. Get all event URLs (start small!)
    event_urls = scrape_event_urls(max_pages=1)  # later: increase to 10, 20, etc.

    all_fights = []

    for event_url in tqdm(event_urls, desc="Events"):
        fights_basic = scrape_fights_for_event(event_url)
        print(f"  {len(fights_basic)} fights found for event")

        for fight in tqdm(fights_basic, desc="  Fights", leave=False):
            fight_url = fight["fight_url"]
            # 2. Scrape detailed stats for this fight
            details = scrape_fight_details(fight_url)

            # Merge dictionaries: basic info + details
            combined = {**fight, **details}
            all_fights.append(combined)

            # Be polite: small delay
            time.sleep(0.3)

        # Slight pause between events
        time.sleep(1.0)

    # 3. Convert to DataFrame and save
    df = pd.DataFrame(all_fights)
    out_path = DATA_DIR / "ufc_fights_raw.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved {len(df)} fights to {out_path}")


def build_events_page_url(page: int) -> str:
    """
    UFCStats completed events are paginated.
    Page 0 is usually the first page without ?page=,
    later pages use ?page=1, ?page=2, etc.

    This helper handles both cases.
    """
    if page == 0:
        return EVENTS_COMPLETED_URL
    return f"{EVENTS_COMPLETED_URL}?page={page}"

def scrape_event_urls(max_pages: int = 3) -> List[str]:
    """
    Scrape UFCStats completed event pages and collect event-detail URLs.

    max_pages: how many pages of completed events to scrape.
               Start small (2–3) while testing.
    """
    all_event_urls: list[str] = []
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


def scrape_fight_details(fight_url: str) -> dict:
    """
    Scrape detailed stats for a single fight.

    Returns a dict with stats for red and blue fighters.
    Some keys may be None if parsing fails.
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
        "blue_td_attempted": None,
    }

    # 1) Determine winner ("Red", "Blue") from fighter blocks
    fighter_blocks = soup.find_all("div", class_="b-fight-details__person")
    if len(fighter_blocks) >= 2:
        red_block = fighter_blocks[0]
        blue_block = fighter_blocks[1]

        red_status = red_block.find("i", class_="b-fight-details__person-status")
        blue_status = blue_block.find("i", class_="b-fight-details__person-status")

        red_text = red_status.get_text(strip=True) if red_status else ""
        blue_text = blue_status.get_text(strip=True) if blue_status else ""

        if "W" in red_text:
            result["winner"] = "Red"
        elif "W" in blue_text:
            result["winner"] = "Blue"

    # 2) Find the "Fight Totals" table
    totals_table = None
    for table in soup.find_all("table", class_="b-fight-details__table"):
        thead = table.find("thead")
        if not thead:
            continue
        header_text = thead.get_text(" ", strip=True).upper()
        # Heuristic: header contains KD and SIG. STR for the totals table
        if "KD" in header_text and "SIG. STR" in header_text:
            totals_table = table
            break

    if not totals_table:
        return result

    tbody = totals_table.find("tbody")
    if not tbody:
        return result

    # The snippet you sent shows ONE row with both fighters' stats in <p> tags
    row = tbody.find("tr", class_="b-fight-details__table-row")
    if not row:
        return result

    cells = row.find_all("td", class_="b-fight-details__table-col")
    # Expecting: [fighters, KD, SIG STR, SIG STR %, TOTAL STR, TD, TD %, SUB, REV, CTRL]
    if len(cells) < 6:
        return result

    # KD column (index 1) → two <p>: red, blue
    kd_ps = cells[1].find_all("p")
    if len(kd_ps) >= 2:
        try:
            result["red_kd"] = int(kd_ps[0].get_text(strip=True) or 0)
            result["blue_kd"] = int(kd_ps[1].get_text(strip=True) or 0)
        except ValueError:
            pass

    # SIG STR column (index 2) → "X of Y" for red/blue
    sig_ps = cells[2].find_all("p")
    if len(sig_ps) >= 2:
        red_sig = sig_ps[0].get_text(strip=True)
        blue_sig = sig_ps[1].get_text(strip=True)
        rs_made, rs_att = parse_made_of(red_sig)
        bs_made, bs_att = parse_made_of(blue_sig)
        result["red_sig_str_landed"] = rs_made
        result["red_sig_str_attempted"] = rs_att
        result["blue_sig_str_landed"] = bs_made
        result["blue_sig_str_attempted"] = bs_att

    # TD column (index 5) → "A of B" for red/blue
    td_ps = cells[5].find_all("p")
    if len(td_ps) >= 2:
        red_td = td_ps[0].get_text(strip=True)
        blue_td = td_ps[1].get_text(strip=True)
        rt_made, rt_att = parse_made_of(red_td)
        bt_made, bt_att = parse_made_of(blue_td)
        result["red_td_landed"] = rt_made
        result["red_td_attempted"] = rt_att
        result["blue_td_landed"] = bt_made
        result["blue_td_attempted"] = bt_att

    return result


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


if __name__ == "__main__":
    scrape_all()


