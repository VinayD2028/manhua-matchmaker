import json
import pandas as pd
from thefuzz import fuzz
from thefuzz import process
from tqdm import tqdm
import os

# Paths
MANGADEX_FILE = "data/raw/mangadex_manhua.json"
ANILIST_FILE = "data/raw/anilist_manhua.json"
OUTPUT_FILE = "data/processed/merged_manhua.json"

def load_data():
    with open(MANGADEX_FILE, 'r', encoding='utf-8') as f:
        md_data = json.load(f)
    try:
        with open(ANILIST_FILE, 'r', encoding='utf-8') as f:
            al_data = json.load(f)
    except FileNotFoundError:
        print("Anilist data not found, proceeding with only MangaDex data.")
        al_data = []
    return md_data, al_data

def normalize_title(title):
    if not title: return ""
    return title.lower().strip()

def merge_datasets():
    print("Starting Entity Resolution (Deduplication)...")
    md_list, al_list = load_data()
    
    # Convert Anilist to DataFrame for faster lookup
    al_df = pd.DataFrame(al_list)
    if not al_df.empty:
        # Extract English title for matching
        al_df['match_title'] = al_df['title'].apply(lambda x: x.get('english') or x.get('romaji') or "")
    
    merged_data = []
    
    for md_item in tqdm(md_list, desc="Merging Records"):
        md_title = md_item.get('title', "")
        best_match_score = 0
        best_al_record = None
        
        # Fuzzy Match against Anilist if available
        if not al_df.empty and md_title:
            # Simple optimization: Filter by first letter could speed up, but dataset is small (500)
            # We compare against all 500 Anilist titles (small enough for brute force 500x500 = 250k checks)
            
            # Using Process.extractOne is actually slower than just iterating if we want specific logic
            # Let's simple check strict exact matches first
            exact_match = al_df[al_df['match_title'].str.lower() == md_title.lower()]
            if not exact_match.empty:
                best_al_record = exact_match.iloc[0].to_dict()
                best_match_score = 100
            else:
                # Fallback to Fuzzy
                for _, al_row in al_df.iterrows():
                    score = fuzz.ratio(normalize_title(md_title), normalize_title(al_row['match_title']))
                    if score > 85 and score > best_match_score: # Threshold 85%
                        best_match_score = score
                        best_al_record = al_row.to_dict()
        
        # Create Golden Record
        golden_record = {
            "id": md_item["id"], # MangaDex ID is primary key
            "title": md_item["title"],
            "description": md_item["description"], # Prefer MangaDex desc usually? Or longest?
            "tags": md_item["tags"],
            "official_en_link": md_item["official_en_link"],
            "cover_art": md_item["cover_art_id"], # We need to resolve this to a URL later
            "year": md_item["year"],
            # Enriched Fields from Anilist
            "rating": best_al_record['averageScore'] if best_al_record else None,
            "popularity": best_al_record['popularity'] if best_al_record else 0,
            "anilist_id": best_al_record['id'] if best_al_record else None,
            # Metadata
            "match_source": "MangaDex+Anilist" if best_al_record else "MangaDex Only",
            "match_score": best_match_score
        }
        
        # Fallback: If MangaDex desc is empty, use Anilist
        if not golden_record['description'] and best_al_record:
            golden_record['description'] = best_al_record.get('description', "")

        merged_data.append(golden_record)

    # Save
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, indent=4, ensure_ascii=False)
    
    print(f"Merged {len(merged_data)} records. Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    merge_datasets()
