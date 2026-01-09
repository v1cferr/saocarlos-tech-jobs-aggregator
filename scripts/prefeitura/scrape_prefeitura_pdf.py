import io
import json
import re
import ssl
from datetime import datetime, timezone

import pdfplumber
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

PDF_URL = "https://saocarlos.sp.gov.br/files/vagas_trabalhador.pdf"

KEYWORDS = [
    "informática",
    "desenvolvedor",
    "programador",
    "suporte técnico",
    "analista de sistemas",
    "analista de ti",
    "sistemas de informação",
    "software",
    "hardware",
    "técnico em informática",
    "infraestrutura de ti",
    "redes de computadores",
]

NEGATIVE_KEYWORDS = [
    "doméstica",
    "motorista",
    "auxiliar",
    "padeiro",
    "vendas",
    "logística",
    "produção",
    "limpeza",
    "farmácia",
    "eletricista",
    "mecânico",
]


class LegacyHttpAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")

        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=ctx,
            **pool_kwargs,
        )


def download_pdf(url: str) -> io.BytesIO:
    session = requests.Session()
    session.mount("https://", LegacyHttpAdapter())

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    # remover verify=False quando o problema do certificado for resolvido
    response = session.get(url, timeout=30, verify=False)
    response.raise_for_status()
    return io.BytesIO(response.content)


def extract_text(pdf_bytes: io.BytesIO) -> str:
    text = []
    with pdfplumber.open(pdf_bytes) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text.append(page_text)
    return "\n".join(text)


def normalize_text(text: str) -> str:
    # remove espaços entre letras isoladas (ex: D e s e n v o l v e d o r -> Desenvolvedor)
    text = re.sub(r"(?<=\b[A-Za-zÀ-ÿ])\s(?=[A-Za-zÀ-ÿ]\b)", "", text)
    # colapsa múltiplos espaços/newlines desnecessários em um único espaço
    text = re.sub(r"\s+", " ", text)
    return text


def is_tech_related(text: str) -> bool:
    lower = text.lower()
    score = 0

    for kw in KEYWORDS:
        if kw in lower:
            score += 1

    for neg in NEGATIVE_KEYWORDS:
        if neg in lower:
            score -= 1

    return score > 0


def filter_tech_vacancies(text: str) -> list[str]:
    # 1. Normaliza tudo para uma linha só
    clean_text = normalize_text(text)

    # 2. Divide usando o padrão de ID da vaga como delimitador
    # Padrão: 8 + 6 dígitos + Espaço + 3 Letras + Espaço + Dígitos (Qtd)
    # Ex: 8734075 RMV 1
    blocks = re.split(r"\s8\d{6}\s[A-Z]{3}\s\d+", clean_text)

    results = []

    for block in blocks:
        block = block.strip()
        if len(block) < 30:
            continue

        if is_tech_related(block):
            results.append(block)

    return results


def main():
    pdf = download_pdf(PDF_URL)
    text = extract_text(pdf)

    # DEBUG: Salvar texto cru
    with open("debug_prefeitura_raw.txt", "w", encoding="utf-8") as f:
        f.write(text)

    # A lógica de normalização será aplicada dentro do filter_tech_vacancies por bloco
    # para não perdermos a separação de vagas.
    matches = filter_tech_vacancies(text)

    output = {
        "source": "Prefeitura de São Carlos - Casa do Trabalhador",
        "url": PDF_URL,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "total_matches": len(matches),
        "results": matches,
    }

    filename = f"data/raw/prefeitura/{datetime.now(timezone.utc).date()}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"{len(matches)} possíveis vagas de TI encontradas.")
    print(f"Arquivo salvo em: {filename}")


if __name__ == "__main__":
    main()
