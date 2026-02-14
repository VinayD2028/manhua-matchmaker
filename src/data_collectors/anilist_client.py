import requests
import json
import os
import time
from datetime import datetime, timedelta

# Constants
URL = 'https://graphql.anilist.co'
OUTPUT_FILE = "data/raw/anilist_manhua.json"

# GraphQL Query Template
QUERY_TEMPLATE = '''
query ($page: Int, $country: CountryCode, $updatedAt: Int) {
  Page (page: $page, perPage: 50) {
    pageInfo {
      total
      currentPage
      lastPage
      hasNextPage
    }
    media (countryOfOrigin: $country, type: MANGA, sort: UPDATED_AT_DESC, updatedAt_greater: $updatedAt) {
      id
      title {
        romaji
        english
        native
      }
      description
      averageScore
      popularity
      genres
      updatedAt
      coverImage {
        large
      }
      siteUrl
    }
  }
}
'''

def load_existing_data():
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading existing data: {e}")
    return []

def fetch_anilist_data():
    existing_data = load_existing_data()
    is_incremental = len(existing_data) > 0
    
    # Use 14 days ago for incremental updates (in Unix timestamp)
    updated_since = int((datetime.utcnow() - timedelta(days=14)).timestamp()) if is_incremental else 0
    
    print(f"Starting ingestion from Anilist (Incremental: {is_incremental}, Since: {updated_since})...")
    
    new_manhua = []
    countries = ["CN", "KR"]
    
    for country in countries:
        print(f"Fetching titles for country: {country}")
        page = 1
        has_next = True
        
        while has_next and page <= 50: # Cap at 50 pages (2500 items per country/run)
            variables = {
                'page': page, 
                'country': country,
                'updatedAt': updated_since if is_incremental else 0
            }
            
            try:
                response = requests.post(URL, json={'query': QUERY_TEMPLATE, 'variables': variables})
                
                if response.status_code == 200:
                    data = response.json()
                    page_data = data['data']['Page']
                    media = page_data['media']
                    
                    if not media:
                        break
                        
                    new_manhua.extend(media)
                    print(f"[{country}] Fetched page {page}. Total items this run: {len(new_manhua)}")
                    
                    has_next = page_data['pageInfo']['hasNextPage']
                    page += 1
                    time.sleep(1) # Rate limit respect
                elif response.status_code == 429:
                    print("Rate limited by Anilist. Sleeping for 60s...")
                    time.sleep(60)
                else:
                    print(f"Error {response.status_code}: {response.text}")
                    break
            except Exception as e:
                print(f"Exception: {e}")
                break

    # Merge and deduplicate
    if is_incremental:
        print(f"Merging {len(new_manhua)} new/updated records with {len(existing_data)} existing records...")
        full_map = {item["id"]: item for item in existing_data}
        for item in new_manhua:
            full_map[item["id"]] = item
        final_list = list(full_map.values())
    else:
        final_list = new_manhua

    # Save to JSON
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_list, f, indent=4, ensure_ascii=False)
        
    print(f"Successfully saved {len(final_list)} total titles to {OUTPUT_FILE}")

if __name__ == "__main__":
    fetch_anilist_data()

