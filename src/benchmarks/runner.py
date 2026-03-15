#!/usr/bin/env python3
"""
Benchmark runner — Faza 4: 24 scenariusze CRUD

Uruchomienie (z katalogu projektu):
  python -m src.benchmarks.runner --volume small
  python -m src.benchmarks.runner --volume small --database postgres
  python -m src.benchmarks.runner --volume medium --trials 5

Wyniki lądują w katalogu results/ jako plik CSV.
"""

import argparse
import csv
import time
import traceback
from datetime import datetime
from pathlib import Path


# ── Konfiguracja połączeń (te same ustawienia co w loaders) ──────────────────

PG_DSN = "host=localhost port=5432 dbname=vod user=vod password=vod123"

MYSQL_CONFIG = dict(
    host="localhost",
    port=3306,
    user="vod",
    password="vod123",
    database="vod",
    charset="utf8mb4",
)

MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_DB   = "vod"

NEO4J_URI  = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "vod12345"

# Liczba prób (trials) dla każdego scenariusza
DEFAULT_TRIALS = 3


# ── Tworzenie połączeń ────────────────────────────────────────────────────────

def connect_postgres():
    import psycopg
    print("  Łączenie z PostgreSQL...", end=" ")
    conn = psycopg.connect(PG_DSN)
    print("OK")
    return conn


def connect_mysql():
    import pymysql
    print("  Łączenie z MySQL...", end=" ")
    conn = pymysql.connect(**MYSQL_CONFIG)
    print("OK")
    return conn


def connect_mongo():
    from pymongo import MongoClient
    print("  Łączenie z MongoDB...", end=" ")
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    print("OK")
    return client, db   # zwracamy oba — client do zamknięcia, db do zapytań


def connect_neo4j():
    from neo4j import GraphDatabase
    print("  Łączenie z Neo4j...", end=" ")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    print("OK")
    return driver


# ── Główna pętla benchmarku ───────────────────────────────────────────────────

def run_benchmarks(
    volume: str,
    databases: list,
    trials: int,
    output_dir: Path,
    phase: str = "all",
) -> None:
    """Uruchamia scenariusze CRUD dla wybranych baz danych.

    phase — filtruje scenariusze po kategorii:
      'all'    — wszystkie 24
      'insert' — tylko I1-I6
      'select' — tylko S1-S6
      'update' — tylko U1-U6
      'delete' — tylko D1-D6
    """

    # Mapowanie phase → category (pole w słowniku scenariusza)
    phase_to_category = {
        "insert": "INSERT",
        "select": "SELECT",
        "update": "UPDATE",
        "delete": "DELETE",
    }
    filter_category = phase_to_category.get(phase)   # None = wszystkie

    all_results = []   # tu gromadzimy wiersze do CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for db_name in databases:
        print(f"\n{'='*60}")
        print(f"  Baza: {db_name.upper()}  |  Wolumen: {volume}  |  Phase: {phase.upper()}")
        print(f"{'='*60}")

        # Ładujemy scenariusze i nawiązujemy połączenie dla tej bazy
        try:
            conn, close_fn, scenarios = _setup_db(db_name)
        except Exception as exc:
            print(f"  [BŁĄD połączenia] {exc}")
            continue

        # Filtruj scenariusze jeśli --phase != all
        if filter_category:
            scenarios = [s for s in scenarios if s.get("category") == filter_category]

        for scenario in scenarios:
            sid      = scenario["id"]
            name     = scenario["name"]
            category = scenario.get("category", "?")
            run_fn      = scenario["run"]
            setup_fn    = scenario.get("setup")
            teardown_fn = scenario.get("teardown")

            print(f"\n  [{sid}] {name}")

            for trial_no in range(1, trials + 1):

                # ─── Setup (przygotowanie danych przed pomiarem) ───────────
                ctx = {}
                if setup_fn:
                    try:
                        ctx = setup_fn(conn) or {}
                    except Exception as exc:
                        print(f"      Trial {trial_no}: [BŁĄD setup] {exc}")
                        traceback.print_exc()
                        if hasattr(conn, "rollback"):
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                        continue

                # ─── Pomiar czasu właściwej operacji ──────────────────────
                try:
                    start_ns = time.perf_counter_ns()
                    run_fn(conn, ctx)
                    elapsed_ns = time.perf_counter_ns() - start_ns
                    elapsed_ms = elapsed_ns / 1_000_000

                    print(f"      Trial {trial_no}: {elapsed_ms:>10.3f} ms")

                    all_results.append({
                        "scenario_id": sid,
                        "name":        name,
                        "category":    category,
                        "database":  db_name,
                        "volume":    volume,
                        "trial":     trial_no,
                        "time_ms":   round(elapsed_ms, 3),
                    })

                except Exception as exc:
                    print(f"      Trial {trial_no}: [BŁĄD run] {exc}")
                    traceback.print_exc()
                    if hasattr(conn, "rollback"):
                        try:
                            conn.rollback()
                        except Exception:
                            pass

                # ─── Teardown (sprzątanie po teście) ──────────────────────
                if teardown_fn:
                    try:
                        teardown_fn(conn, ctx)
                    except Exception as exc:
                        print(f"      Trial {trial_no}: [BŁĄD teardown] {exc}")

        # Zamknij połączenie z bazą
        try:
            close_fn()
        except Exception:
            pass

    # ── Zapisz wyniki do CSV ──────────────────────────────────────────────────
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"results_{volume}_{phase}_{timestamp}.csv"

    fieldnames = ["scenario_id", "name", "category", "database", "volume", "trial", "time_ms"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)

    print(f"\n{'='*60}")
    print(f"  Wyniki zapisane: {csv_path}")
    print(f"  Łączna liczba pomiarów: {len(all_results)}")
    print(f"{'='*60}\n")


# ── Pomocnicza funkcja — setup połączenia i import scenariuszy ────────────────

def _setup_db(db_name: str):
    """Zwraca (conn, close_fn, scenarios) dla danej bazy."""

    if db_name == "postgres":
        from src.benchmarks.pg_scenarios import SCENARIOS
        conn = connect_postgres()
        return conn, conn.close, SCENARIOS

    elif db_name == "mysql":
        from src.benchmarks.mysql_scenarios import SCENARIOS
        conn = connect_mysql()
        return conn, conn.close, SCENARIOS

    elif db_name == "mongo":
        from src.benchmarks.mongo_scenarios import SCENARIOS
        client, db = connect_mongo()
        # Scenariusze Mongo dostają obiekt 'db' (Database), nie client
        return db, client.close, SCENARIOS

    elif db_name == "neo4j":
        from src.benchmarks.neo4j_scenarios import SCENARIOS
        driver = connect_neo4j()
        return driver, driver.close, SCENARIOS

    else:
        raise ValueError(f"Nieznana baza: {db_name}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="ZTBD Faza 4 — Benchmarki CRUD (24 scenariusze × 4 bazy)"
    )
    parser.add_argument(
        "--volume",
        choices=["small", "medium", "large"],
        default="small",
        help="Wolumen danych załadowanych do baz (default: small)",
    )
    parser.add_argument(
        "--database",
        choices=["all", "postgres", "mysql", "mongo", "neo4j"],
        default="all",
        help="Która baza danych (default: all)",
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=DEFAULT_TRIALS,
        help=f"Liczba prób per scenariusz (default: {DEFAULT_TRIALS})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("results"),
        help="Katalog na wyniki CSV (default: results/)",
    )
    parser.add_argument(
        "--phase",
        choices=["all", "insert", "select", "update", "delete"],
        default="all",
        help="Która faza CRUD (default: all)",
    )

    args = parser.parse_args()

    databases = (
        ["postgres", "mysql", "mongo", "neo4j"]
        if args.database == "all"
        else [args.database]
    )

    run_benchmarks(args.volume, databases, args.trials, args.output_dir, args.phase)


if __name__ == "__main__":
    main()
