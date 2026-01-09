from __future__ import annotations

import json
import re
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from scripts.cezcomrh.scrape_vaga_detail import scrape_vaga_detail

BASE_URL = "https://cezcomrh.tweezer.jobs"
SEARCH_ENDPOINT = "/candidato/vaga/buscar_vaga/json/"

HEADERS = {
    "User-Agent": "saocarlos-tech-jobs-aggregator/0.1 (+https://github.com/v1cferr)",
    "Accept": "application/json",
    "X-Requested-With": "XMLHttpRequest",
}

OUTPUT_DIR = Path("data/raw/cezcomrh")


def fetch_page(page: int, funcao: str, cidade: str) -> dict:
    params = {
        "page": page,
        "funcao": funcao,
        "cidade": cidade,
        "force_pesquisa": "true",
        "_": int(time.time() * 1000),
    }

    response = requests.get(
        BASE_URL + SEARCH_ENDPOINT,
        params=params,
        headers=HEADERS,
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def parse_vagas(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    vagas = []

    for card in soup.select(".card"):
        title_el = card.select_one(".card-title")
        location_el = card.select_one(".fa-map-marker-alt")
        urgent_el = card.select_one(".vaga-urgente")

        # Discovery Phase
        # Strategy 1: Look for a direct link or a social share link containing the URL
        link_el = card.select_one("a[href*='/candidato/vaga/ver_vaga/']")
        vaga_url = None
        vaga_id = None

        if link_el:
            href = link_el["href"]
            if "url=" in href:  # LinkedIn/Twitter style

                parsed = urllib.parse.urlparse(href)
                query = urllib.parse.parse_qs(parsed.query)
                if "url" in query:
                    vaga_url = query["url"][0]
            elif "u=" in href:  # Facebook style
                parsed = urllib.parse.urlparse(href)
                query = urllib.parse.parse_qs(parsed.query)
                if "u" in query:
                    vaga_url = query["u"][0]
            elif "status=" in href:  # Twitter style alternative
                # naive extraction if needed, or just rely on regex if logic gets complex
                # But usually the above covering common cases is enough.
                # Let's use a regex to find the url inside the string for robustness

                match = re.search(
                    r"(https?://[^ ]+/candidato/vaga/ver_vaga/[\w-]+)", href
                )
                if match:
                    vaga_url = match.group(1)
            else:
                # Direct link (relative or absolute)
                if href.startswith("http"):
                    vaga_url = href
                else:
                    vaga_url = BASE_URL + href

        # Strategy 2: Fallback to onclick (legacy/alternative)
        if not vaga_url:
            onclick = card.select_one("[onclick*='ver_vaga']")
            if onclick:

                match = re.search(r"ver_vaga\('([\w-]+)'\)", onclick["onclick"])
                if match:
                    vaga_id = match.group(1)
                    vaga_url = f"{BASE_URL}/candidato/vaga/ver_vaga/{vaga_id}"

        # Clean up URL (handle double slashes if present)
        if vaga_url:
            vaga_url = vaga_url.replace(f"{BASE_URL}//", f"{BASE_URL}/")
            if not vaga_id:
                vaga_id = vaga_url.rstrip("/").split("/")[-1]

        vagas.append(
            {
                "id": vaga_id,
                "title": title_el.get_text(strip=True) if title_el else None,
                "location": (
                    location_el.find_parent("span").get_text(strip=True)
                    if location_el
                    else None
                ),
                "urgent": urgent_el is not None,
                "url": vaga_url,
                "source": "CezcomRH",
            }
        )

    return vagas


def main() -> None:
    funcao = "tecnologia"
    cidade = "São Carlos"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_vagas: list[dict] = []
    page = 1

    while True:
        data = fetch_page(page, funcao, cidade)

        vagas = parse_vagas(data.get("html", ""))

        # Enrichment Phase
        for vaga in vagas:
            if vaga["url"]:
                try:
                    print(f"  Enriching: {vaga['url']}...")
                    details = scrape_vaga_detail(vaga["url"])
                    vaga.update(details)
                    time.sleep(0.5)  # Be nice to the server
                except Exception as e:
                    print(f"  Error enriching {vaga['url']}: {e}")
                    vaga["error"] = str(e)
            else:
                print(f"  Skipping enrichment for {vaga['title']} (no URL)")

        all_vagas.extend(vagas)

        if not data.get("has_next"):
            break

        page += 1
        time.sleep(1)

    payload = {
        "source": "CezcomRH",
        "query": {
            "funcao": funcao,
            "cidade": cidade,
        },
        "scraped_at": datetime.now().isoformat(),
        "total": len(all_vagas),
        "vacancies": all_vagas,
    }

    output_file = OUTPUT_DIR / f"{datetime.now().date()}.json"
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"[OK] {len(all_vagas)} vagas salvas em {output_file}")


if __name__ == "__main__":
    main()
