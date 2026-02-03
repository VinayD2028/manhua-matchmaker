import requests

MANGADEX_BASE = "https://api.mangadex.org"

def get_chapters(manga_id, limit=100):
    """
    Fetch latest chapters for a manga (English or Chinese).
    """
    params = {
        "limit": limit,
        "manga": manga_id,
        "translatedLanguage[]": ["en", "zh"],
        "order[chapter]": "asc", # Start from Ch 1
        "contentRating[]": ["safe", "suggestive", "erotica", "pornographic"]
    }
    try:
        r = requests.get(f"{MANGADEX_BASE}/chapter", params=params)
        if r.status_code == 200:
            return r.json().get('data', [])
        return []
    except:
        return []

def get_chapter_pages(chapter_id):
    """
    Get the image URLs for a specific chapter via At-Home Server.
    """
    try:
        # 1. Get Base URL
        r = requests.get(f"{MANGADEX_BASE}/at-home/server/{chapter_id}")
        if r.status_code != 200:
            return []
            
        data = r.json()
        base_url = data.get('baseUrl')
        chapter_hash = data.get('chapter', {}).get('hash')
        filenames = data.get('chapter', {}).get('data', []) # High Quality
        
        # 2. Construct URLs
        page_urls = []
        for file in filenames:
            # Format: {baseUrl}/data/{hash}/{filename}
            url = f"{base_url}/data/{chapter_hash}/{file}"
            page_urls.append(url)
            
        return page_urls
    except:
        return []
