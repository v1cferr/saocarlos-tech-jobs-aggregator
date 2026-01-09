import json
from html import unescape

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "saocarlos-tech-jobs-aggregator/0.1 (+https://github.com/v1cferr)",
}


def html_to_text(html: str) -> str:
    if not html:
        return None
    soup = BeautifulSoup(unescape(html), "html.parser")
    return soup.get_text("\n", strip=True)


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
                data["description_html"] = ld_json.get("description")  # Raw HTML
                data["contract_type"] = ld_json.get("employmentType")

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
        # Helper to unescape if found via other means later
        pass

    if not data.get("description_html"):
        # Look for the container having "Descrição detalhada"
        desc_header = soup.find(string=lambda t: t and "Descrição detalhada" in t)
        if desc_header:
            # The description text usually follows this header or is in the same container
            container = desc_header.find_parent("div")
            if container:
                # Get all text from this container, effectively
                # fallback to saving text as html representation if real html unavailable
                data["description_html"] = container.decode_contents()

    # Final cleanup and normalization
    if data.get("company"):
        data["company"] = unescape(data.get("company"))

    description_html = data.get("description_html")
    # Clean text version
    description_text = html_to_text(description_html) if description_html else None

    return {
        "title": data.get("title"),
        "company": data.get("company"),
        "location": data.get("location"),
        "contract_type": data.get("contract_type"),
        "description_html": description_html,
        "description_text": description_text,
        "requirements": None,  # Often mixed in description
    }
