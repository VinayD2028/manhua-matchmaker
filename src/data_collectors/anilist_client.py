import requests
import json
import os
import time

# Constants
URL = 'https://graphql.anilist.co'
OUTPUT_FILE = "data/raw/anilist_manhua.json"

# GraphQL Query
QUERY = '''
query ($page: Int) {
  Page (page: $page, perPage: 50) {
    pageInfo {
      total
      currentPage
      lastPage
      hasNextPage
    }
    media (countryOfOrigin: "CN", type: MANGA, sort: POPULARITY_DESC) {
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
      coverImage {
        large
      }
      siteUrl
    }
  }
}
'''

def fetch_anilist_data():
    print("Starting ingestion from Anilist (Top Popular Manhua)...")
    
    manhua_list = []
    page = 1
    has_next = True
    max_pages = 10 # 10 pages * 50 items = 500 items (Match dev dataset)

    while has_next and page <= max_pages:
        variables = {'page': page}
        
        try:
            response = requests.post(URL, json={'query': QUERY, 'variables': variables})
            
            if response.status_code == 200:
                data = response.json()
                page_data = data['data']['Page']
                media = page_data['media']
                
                manhua_list.extend(media)
                
                print(f"Fetched page {page}. Total items: {len(manhua_list)}")
                
                has_next = page_data['pageInfo']['hasNextPage']
                page += 1
                
                # Polite rate limiting (Anilist permits 90 req/min)
                time.sleep(1) 
                
            else:
                print(f"Error {response.status_code}: {response.text}")
                break
                
        except Exception as e:
            print(f"Exception: {e}")
            break

    # Save to JSON
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(manhua_list, f, indent=4, ensure_ascii=False)
        
    print(f"Successfully saved {len(manhua_list)} titles to {OUTPUT_FILE}")

if __name__ == "__main__":
    fetch_anilist_data()
