import json

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "saocarlos-tech-jobs-aggregator/0.1 (+https://github.com/v1cferr)",
}


def scrape_vaga_detail(url: str) -> dict:
    response = requests.get(url, headers=HEADERS, timeout=15)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    data = {}

    # Strategy 1: JSON-LD (Schema.org) - Best for SEO-friendly sites
    ld_script = soup.find("script", type="application/ld+json")
    if ld_script:
        try:
            ld_json = json.loads(ld_script.string, strict=False)
            # Handle list of schemas or single schema
            if isinstance(ld_json, list):
                ld_json = next(
                    (item for item in ld_json if item.get("@type") == "JobPosting"),
                    None,
                )

            if ld_json and ld_json.get("@type") == "JobPosting":
                data["title"] = ld_json.get("title")
                data["description"] = ld_json.get("description")  # Often contains HTML

                org = ld_json.get("hiringOrganization")
                if isinstance(org, dict):
                    data["company"] = org.get("name")

                loc = ld_json.get("jobLocation")
                if isinstance(loc, dict):
                    addr = loc.get("address")
                    if isinstance(addr, dict):
                        data["location"] = (
                            f"{addr.get('addressLocality', '')} - {addr.get('addressRegion', '')}"
                        )

                data["source_schema"] = "JobPosting"
        except json.JSONDecodeError:
            pass

    # Strategy 2: HTML Fallback (if JSON-LD is missing or incomplete)
    if not data.get("title"):
        h2 = soup.find("h2")  # Title often in first H2
        if h2:
            data["title"] = h2.get_text(strip=True)

    if not data.get("company"):
        # Based on "CEZCOM CONSULTORIA & RH LTDA" appearing in text,
        # usually it's in a specific div or just derived.
        # For now, let's skip specific HTML parsing for company if JSON-LD failed,
        # as the generic classes are hard to pin down without more samples.
        pass

    if not data.get("description"):
        # Look for the container having "Descrição detalhada"
        desc_header = soup.find(string=lambda t: t and "Descrição detalhada" in t)
        if desc_header:
            # The description text usually follows this header or is in the same container
            container = desc_header.find_parent("div")
            if container:
                # Get all text from this container, effectively
                data["description"] = container.get_text("\n", strip=True)

    return {
        "title": data.get("title"),
        "company": data.get("company"),
        "location": data.get("location"),
        "contract_type": None,  # Not clearly in JSON-LD usually (maybe employmentType)
        "description": data.get("description"),
        "requirements": None,  # Often mixed in description
    }
