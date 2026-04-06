import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime

def scrape_and_save(url, output_dir="./data"):
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Fetch the page
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # Parse HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        paragraphs = soup.find_all('p')
        
        # Extract data
        data = {
            "url": url,
            "scraped_at": datetime.now().isoformat(),
            "total_paragraphs": len(paragraphs),
            "paragraphs": []
        }
        
        for i, p in enumerate(paragraphs, 1):
            text = p.get_text(strip=True)
            if text:  # Skip empty tags
                data["paragraphs"].append({
                    "index": i,
                    "text": text,
                    "html": str(p)
                })
        
        # Save as JSON
        json_path = os.path.join(output_dir, "paragraphs.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        # Save as readable text
        txt_path = os.path.join(output_dir, "paragraphs.txt")
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(f"Source: {url}\n")
            f.write(f"Scraped: {data['scraped_at']}\n")
            f.write(f"Total: {data['total_paragraphs']} paragraphs\n")
            f.write("="*50 + "\n\n")
            
            for p in data["paragraphs"]:
                f.write(f"[{p['index']}] {p['text']}\n\n")
        
        print(f"✓ Saved {len(data['paragraphs'])} paragraphs to:")
        print(f"  - {json_path}")
        print(f"  - {txt_path}")
        
        return data
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return None

# Run it
if __name__ == "__main__":
    url = "https://thelovelessguru.livejournal.com/33614.html"
    scrape_and_save(url)