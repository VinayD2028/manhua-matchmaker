import requests

MANGADEX_BASE = "https://api.mangadex.org"

def get_chapters(manga_id, limit_per_req=100):
    """
    Fetch ALL chapters for a manga (English or Chinese) using pagination.
    """
    all_chapters = []
    offset = 0
    
    while True:
        params = {
            "limit": limit_per_req,
            "offset": offset,
            "manga": manga_id,
            "translatedLanguage[]": ["en", "zh"],
            "order[chapter]": "asc", 
            "contentRating[]": ["safe", "suggestive", "erotica", "pornographic"]
        }
        try:
            r = requests.get(f"{MANGADEX_BASE}/chapter", params=params)
            if r.status_code == 200:
                data = r.json().get('data', [])
                if not data:
                    break
                    
                all_chapters.extend(data)
                
                # Check if we reached the end
                total_items = r.json().get('total', 0)
                if len(all_chapters) >= total_items:
                    break
                
                offset += len(data)
            else:
                break
        except:
            break
            
    return all_chapters

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

def fetch_image_bytes(url):
    """
    Fetch an image from MangaDex with the necessary headers to bypass hotlinking protection.
    """
    headers = {
        "Referer": "https://mangadex.org",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            return r.content
    except Exception as e:
        print(f"Error fetching image: {e}")
    return None
