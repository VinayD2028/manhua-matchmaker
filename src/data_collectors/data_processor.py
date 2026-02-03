import json
import pandas as pd
from rapidfuzz import fuzz
from tqdm import tqdm
import os
from collections import defaultdict

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
    print("Starting Optimized Entity Resolution...")
    md_list, al_list = load_data()
    
    # PRE-PROCESS ANILIST FOR SPEED
    # 1. Exact map for instant link (O(1))
    # 2. Blocked map by first letter for narrowed fuzzy search
    al_exact_map = {}
    al_blocked_map = defaultdict(list)
    
    for al_item in al_list:
        eng = normalize_title(al_item['title'].get('english'))
        rom = normalize_title(al_item['title'].get('romaji'))
        
        if eng:
            al_exact_map[eng] = al_item
            al_blocked_map[eng[0]].append(al_item)
        if rom:
            al_exact_map[rom] = al_item
            if not eng or rom[0] != eng[0]:
                al_blocked_map[rom[0]].append(al_item)

    merged_data = []
    
    for md_item in tqdm(md_list, desc="Merging Records"):
        md_title = md_item.get('title', "")
        norm_md_title = normalize_title(md_title)
        
        best_match_score = 0
        best_al_record = None
        
        # Priority 1: Exact Match (Fast)
        if norm_md_title and norm_md_title in al_exact_map:
            best_al_record = al_exact_map[norm_md_title]
            best_match_score = 100
        elif norm_md_title:
            # Priority 2: Blocked Fuzzy Match (only check records starting with same char)
            first_char = norm_md_title[0]
            candidates = al_blocked_map.get(first_char, [])
            
            for al_record in candidates:
                al_titles = [
                    normalize_title(al_record['title'].get('english')),
                    normalize_title(al_record['title'].get('romaji'))
                ]
                
                for t in al_titles:
                    if not t: continue
                    score = fuzz.ratio(norm_md_title, t)
                    if score > 85 and score > best_match_score:
                        best_match_score = score
                        best_al_record = al_record
        
        # Create Golden Record
        golden_record = {
            "id": md_item["id"],
            "title": md_item["title"],
            "alt_titles": md_item.get("alt_titles", []),
            "description": md_item["description"],
            "tags": md_item["tags"],
            "official_en_link": md_item.get("official_en_link"),
            "cover_art": md_item.get("cover_art_id"),
            "year": md_item["year"],
            # Enriched from Anilist
            "rating": best_al_record['averageScore'] if best_al_record else None,
            "popularity": best_al_record['popularity'] if best_al_record else 0,
            "anilist_id": best_al_record['id'] if best_al_record else None,
            "match_source": "MangaDex+Anilist" if best_al_record else "MangaDex Only",
            "match_score": best_match_score
        }
        
        # Fallback metadata
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
