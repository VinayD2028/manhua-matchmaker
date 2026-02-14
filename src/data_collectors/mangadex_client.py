import requests
import pandas as pd
import time
import os
import json
from datetime import datetime, timedelta
from tqdm import tqdm

# Constants
BASE_URL = "https://api.mangadex.org"
OUTPUT_FILE = "data/raw/mangadex_manhua.json"
LIMIT = 100  # Max allowed by API per request
OFFSET_LIMIT = 10000 # MangaDex hard limit for offset
TOTAL_TO_FETCH = 10000 # Updated from 20000 to obey API limits
RATE_LIMIT_SLEEP = 0.25  # 4 requests per second (safe side of 5/sec)

def load_existing_data():
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading existing data: {e}")
    return []

def fetch_manhua():
    existing_data = load_existing_data()
    existing_ids = {item["id"] for item in existing_data}
    
    is_incremental = len(existing_data) > 0
    if is_incremental:
        # Fetch items updated in the last 14 days to catch any missed updates
        since_date = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%S")
        print(f"Starting INCREMENTAL ingestion (Updated since: {since_date})...")
        order_key = "updatedAt"
    else:
        print(f"Starting FULL ingestion (Target: {TOTAL_TO_FETCH})...")
        order_key = "followedCount"
        since_date = None

    manhua_list = []
    offset = 0
    
    params = {
        "limit": LIMIT,
        "offset": offset,
        "includedTagsMode": "AND",
        "excludedTagsMode": "OR",
        "originalLanguage[]": ["zh", "zh-hk", "ko"],
        "order[" + order_key + "]": "desc",
        "includes[]": ["cover_art", "author", "artist"]
    }
    
    if since_date:
        params["updatedAtSince"] = since_date

    pbar = tqdm(total=TOTAL_TO_FETCH if not is_incremental else 1000)
    
    while offset < OFFSET_LIMIT:
        try:
            params["offset"] = offset
            response = requests.get(f"{BASE_URL}/manga", params=params)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("data", [])
                
                if not results:
                    print(f"No more results found at offset {offset}. Ingestion complete.")
                    break
                
                new_items_count = 0
                for manga in results:
                    attrs = manga.get("attributes", {})
                    
                    # Extract Data (Existing logic)
                    title = attrs.get("title", {}).get("en")
                    if not title:
                        vals = list(attrs.get("title", {}).values())
                        title = vals[0] if vals else "Unknown Title"
                        
                    desc = attrs.get("description", {}).get("en", "")
                    if not desc and attrs.get("description"):
                         desc = list(attrs.get("description", {}).values())[0]

                    tags = [t["attributes"]["name"]["en"] for t in attrs.get("tags", []) if "attributes" in t and "name" in t["attributes"]]
                    
                    links = attrs.get("links", {}) or {}
                    official_eng_link = links.get("engtl")
                    raw_link = links.get("raw")
                    
                    cover_filename = None
                    for rel in manga.get("relationships", []):
                        if rel["type"] == "cover_art" and "attributes" in rel:
                             cover_filename = rel["attributes"].get("fileName")
                             break
                    
                    alt_titles_list = []
                    eng_alt_title = None
                    for alt in attrs.get("altTitles", []):
                        for lang, val in alt.items():
                            if lang == "en":
                                alt_titles_list.append(val)
                                if not eng_alt_title:
                                    eng_alt_title = val
                            elif lang in ["ko-ro", "zh-ro"]:
                                alt_titles_list.append(val)
                    
                    if eng_alt_title and not attrs.get("title", {}).get("en"):
                        title = eng_alt_title

                    entry = {
                        "id": manga["id"],
                        "title": title,
                        "alt_titles": alt_titles_list,
                        "description": desc,
                        "tags": tags,
                        "status": attrs.get("status"),
                        "year": attrs.get("year"),
                        "rating": attrs.get("contentRating"), 
                        "official_en_link": official_eng_link,
                        "raw_link": raw_link,
                        "cover_art_id": cover_filename,
                        "updated_at": attrs.get("updatedAt")
                    }
                    
                    manhua_list.append(entry)
                    new_items_count += 1
                
                offset += len(results)
                pbar.update(len(results))
                time.sleep(RATE_LIMIT_SLEEP)
                
            elif response.status_code == 400:
                print(f"Reached API limit or invalid request at offset {offset}. Stopping.")
                break
            elif response.status_code == 429:
                print("Rate limited. Sleeping for 30s...")
                time.sleep(30)
            else:
                print(f"Error {response.status_code}: {response.text}")
                break
                
        except Exception as e:
            print(f"Exception occurred: {e}")
            break

    pbar.close()
    
    # Merge and Deduplicate
    if is_incremental:
        print(f"Merging {len(manhua_list)} new/updated records with {len(existing_data)} existing records...")
        # Dictionary to store merged items, new items overwrite old ones by ID
        full_map = {item["id"]: item for item in existing_data}
        for item in manhua_list:
            full_map[item["id"]] = item
        final_list = list(full_map.values())
    else:
        final_list = manhua_list

    # Save to JSON
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_list, f, indent=4, ensure_ascii=False)
        
    print(f"Successfully saved {len(final_list)} total titles to {OUTPUT_FILE}")

if __name__ == "__main__":
    fetch_manhua()

