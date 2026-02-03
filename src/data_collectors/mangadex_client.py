import requests
import pandas as pd
import time
import os
import json
from tqdm import tqdm

# Constants
BASE_URL = "https://api.mangadex.org"
OUTPUT_FILE = "data/raw/mangadex_manhua.json"
LIMIT = 100  # Max allowed by API per request
TOTAL_TO_FETCH = 500  # Target number of titles (Reduced for Dev Speed)
RATE_LIMIT_SLEEP = 0.25  # 4 requests per second (safe side of 5/sec)

def fetch_manhua():
    print(f"Starting ingestion of {TOTAL_TO_FETCH} Manhua titles from MangaDex...")
    
    manhua_list = []
    offset = 0
    
    # Filter for Chinese (zh) and Hong Kong Chinese (zh-hk)
    # Order by 'followedCount' desc to get popular ones first
    params = {
        "limit": LIMIT,
        "offset": offset,
        "includedTagsMode": "AND",
        "excludedTagsMode": "OR",
        "originalLanguage[]": ["zh", "zh-hk"],
        "order[followedCount]": "desc",
        "includes[]": ["cover_art", "author", "artist"] # Expand metadata if needed
    }

    pbar = tqdm(total=TOTAL_TO_FETCH)
    
    while len(manhua_list) < TOTAL_TO_FETCH:
        try:
            params["offset"] = offset
            response = requests.get(f"{BASE_URL}/manga", params=params)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("data", [])
                
                if not results:
                    print("No more results found.")
                    break
                
                for manga in results:
                    attrs = manga.get("attributes", {})
                    
                    # Extract English Title (fallback to any title)
                    title = attrs.get("title", {}).get("en")
                    if not title:
                        # Grab first available title
                        vals = list(attrs.get("title", {}).values())
                        title = vals[0] if vals else "Unknown Title"
                        
                    # Extract Description
                    desc = attrs.get("description", {}).get("en", "")
                    if not desc and attrs.get("description"):
                         # Fallback to first available description
                         desc = list(attrs.get("description", {}).values())[0]

                    # Extract Tag names safely
                    tags = [t["attributes"]["name"]["en"] for t in attrs.get("tags", []) if "attributes" in t and "name" in t["attributes"]]
                    
                    # Extract External Links (Official TL)
                    links = attrs.get("links", {}) or {}
                    official_eng_link = links.get("engtl")
                    raw_link = links.get("raw")
                    
                    # Extract Cover Art Filename
                    # We need the relationship with type 'cover_art' and get its attributes->fileName
                    cover_filename = None
                    for rel in manga.get("relationships", []):
                        if rel["type"] == "cover_art" and "attributes" in rel:
                             cover_filename = rel["attributes"].get("fileName")
                             break
                    
                    entry = {
                        "id": manga["id"],
                        "title": title,
                        "description": desc,
                        "tags": tags,
                        "status": attrs.get("status"),
                        "year": attrs.get("year"),
                        "rating": attrs.get("contentRating"), # safe, suggestive, erotica
                        "official_en_link": official_eng_link,
                        "raw_link": raw_link,
                        "cover_art_id": cover_filename # Storing filename directly now
                    }
                    manhua_list.append(entry)
                
                count_fetched = len(results)
                offset += count_fetched
                pbar.update(count_fetched)
                
                # Respect Rate Limit
                time.sleep(RATE_LIMIT_SLEEP)
                
            else:
                print(f"Error {response.status_code}: {response.text}")
                time.sleep(5) # Backoff on error
                
        except Exception as e:
            print(f"Exception occurred: {e}")
            break

    pbar.close()
    
    # Save to JSON
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(manhua_list, f, indent=4, ensure_ascii=False)
        
    print(f"Successfully saved {len(manhua_list)} titles to {OUTPUT_FILE}")

if __name__ == "__main__":
    fetch_manhua()
