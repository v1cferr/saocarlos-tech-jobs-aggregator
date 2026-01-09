from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

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

        onclick = card.select_one("button[onclick]")
        vaga_id = None
        if onclick:
            # ver_vaga('UUID')
            raw = onclick["onclick"]
            vaga_id = raw.split("'")[1]

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
                "url": (
                    f"{BASE_URL}/candidato/vaga/ver_vaga/{vaga_id}" if vaga_id else None
                ),
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
        all_vagas.extend(vagas)

        if not data.get("has_next"):
            break

        page += 1
        time.sleep(0.5)  # educação básica com o servidor

    payload = {
        "source": "CezcomRH",
        "query": {
            "funcao": funcao,
            "cidade": cidade,
        },
        "scraped_at": datetime.utcnow().isoformat(),
        "total": len(all_vagas),
        "vacancies": all_vagas,
    }

    output_file = OUTPUT_DIR / f"{datetime.now().date()}.json"
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"[OK] {len(all_vagas)} vagas salvas em {output_file}")


if __name__ == "__main__":
    main()
