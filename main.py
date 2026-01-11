#!/usr/bin/env python3
"""
Orquestrador de Coleta de Vagas - São Carlos Tech Jobs Aggregator

Como adicionar um novo coletor (scraper):
1. Crie um arquivo .py dentro da pasta 'scripts/' (ex: scripts/linkedin/scrape.py).
2. Defina uma função pública `run()` que execute a coleta.
3. Pronto! O script será descoberto automaticamente ao rodar `python main.py --all`.
"""
import argparse
import importlib
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def descobrir_scrapers():
    """Descobre automaticamente scripts com função run() na pasta scripts/."""
    scrapers = {}
    root = Path(__file__).parent / "scripts"

    if not root.exists():
        logging.warning("Pasta 'scripts' não encontrada.")
        return scrapers

    for arquivo in root.rglob("*.py"):
        if arquivo.name == "__init__.py" or "pycache" in str(arquivo):
            continue

        # Converte caminho do arquivo para módulo Python
        relativo = arquivo.relative_to(root.parent)
        modulo_path = str(relativo).replace("/", ".").replace(".py", "")

        try:
            mod = importlib.import_module(modulo_path)
            if hasattr(mod, "run") and callable(mod.run):
                # Usa nome da pasta pai (ex: 'prefeitura') ou nome do arquivo
                nome = relativo.parts[1] if len(relativo.parts) > 2 else arquivo.stem
                scrapers[nome] = modulo_path
        except Exception as e:
            logging.debug(f"Ignorando {modulo_path}: {e}")

    return scrapers


def executar_fonte(nome, modulo_path, dry_run):
    if dry_run:
        logging.info(f"[SIMULAÇÃO] Rodaria: {nome} ({modulo_path})")
        return True

    try:
        logging.info(f"Executando: {nome}...")
        importlib.import_module(modulo_path).run()
        return True
    except Exception as e:
        logging.exception(f"Erro ao rodar {nome}: {e}")
        return False


def main():
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=logging.INFO,
        datefmt="%H:%M:%S",
    )

    fontes = descobrir_scrapers()
    if not fontes:
        logging.error(
            "Nenhum scraper encontrado! Crie um script com função run() em 'scripts/'."
        )
        sys.exit(1)

    p = argparse.ArgumentParser(description="Orquestrador de Coleta de Vagas")
    p.add_argument("--all", action="store_true", help="Rodar todos os scrapers")
    p.add_argument(
        "--source",
        "-s",
        nargs="+",
        choices=fontes.keys(),
        help="Rodar fontes específicas",
    )
    p.add_argument("--dry-run", action="store_true", help="Apenas simular a execução")
    p.add_argument("--concurrent", "-c", action="store_true", help="Rodar em paralelo")
    p.add_argument("--list", action="store_true", help="Listar fontes disponíveis")
    args = p.parse_args()

    if args.list:
        print("Fontes disponíveis:", ", ".join(fontes.keys()))
        return

    alvos = args.source if args.source else list(fontes.keys())
    if not args.all and not args.source:
        alvos = list(fontes.keys())  # Padrão: rodar tudo

    logging.info(f"Fontes selecionadas: {alvos}")

    sucesso = []
    executor = ThreadPoolExecutor() if args.concurrent else None

    if args.concurrent:
        with executor as ex:
            futs = {
                ex.submit(executar_fonte, t, fontes[t], args.dry_run): t for t in alvos
            }
            sucesso = [t for t, f in futs.items() if f.result()]
    else:
        sucesso = [t for t in alvos if executar_fonte(t, fontes[t], args.dry_run)]

    logging.info(f"Concluído: {len(sucesso)}/{len(alvos)} com sucesso.")
    sys.exit(0 if len(sucesso) == len(alvos) else 1)


if __name__ == "__main__":
    main()
