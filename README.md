# ZTBD VOD Platform — Test Data Generator

Projekt zaliczeniowy z przedmiotu **Zaawansowane Technologie Baz Danych (ZTBD)**.

Aplikacja generuje syntetyczne dane testowe dla platformy VOD i ładuje je do czterech różnych systemów baz danych w celu porównania ich wydajności i podejść do przechowywania danych.

---

## Bazy danych

| Baza danych    | Wersja | Port  | Paradygmat                |
|----------------|--------|-------|---------------------------|
| PostgreSQL     | 17     | 5432  | Relacyjna                 |
| MySQL          | 8.0    | 3306  | Relacyjna                 |
| MongoDB        | 8.0    | 27017 | Dokumentowa (NoSQL)       |
| Neo4j          | 2026.x | 7687  | Grafowa                   |

---

## Struktura danych

Schemat modeluje platformę streamingową z następującymi encjami:

- **Users** — konta użytkowników
- **Profiles** — profile w ramach konta (podobnie jak w Netflix)
- **Subscriptions** — plany subskrypcji (Basic / Standard / Premium)
- **Payments** — historia płatności
- **Content** — filmy i seriale
- **Seasons / Episodes** — sezony i odcinki seriali
- **People** — aktorzy, reżyserzy, scenarzyści
- **Watch History** — historia oglądania
- **My List** — lista zapisanych treści
- **Ratings** — oceny i recenzje

---

## Rozmiary danych

| Wolumen  | Użytkownicy | Treści  | Historia oglądania | Przeznaczenie        |
|----------|-------------|---------|-------------------|----------------------|
| `small`  | 1 000       | 500     | 50 000            | Szybkie testy        |
| `medium` | 100 000     | 10 000  | 5 000 000         | Testy standardowe    |
| `large`  | 1 000 000   | 30 000  | 50 000 000        | Testy wydajnościowe  |

---

## Wymagania

- Python 3.x
- Docker & Docker Compose

---

## Uruchomienie

### 1. Uruchom kontenery z bazami danych

```bash
docker compose up -d
```

### 2. Utwórz i aktywuj środowisko wirtualne

```bash
python -m venv venv

# Linux/macOS
source venv/bin/activate

# Windows
venv\Scripts\activate
```

### 3. Zainstaluj zależności

```bash
pip install -r requirements.txt
```

### 4. Wygeneruj dane

```bash
python main.py generate --volume small
```

### 5. Załaduj dane do baz

```bash
# Wszystkie bazy danych naraz
python main.py load --volume small

# Lub konkretna baza
python main.py load --volume small --database postgres
python main.py load --volume small --database mysql
python main.py load --volume small --database mongo
python main.py load --volume small --database neo4j
```

### Pełny przykład (jedna komenda)

```bash
python main.py generate --volume small && python main.py load --volume small
```

---

## Opcje CLI

### `generate`

```
python main.py generate --volume {small|medium|large} [--seed SEED] [--data-dir DIR]
```

| Opcja        | Opis                                      | Domyślna |
|--------------|-------------------------------------------|----------|
| `--volume`   | Rozmiar danych                            | wymagane |
| `--seed`     | Ziarno losowości (dla reprodukowalności)  | 42       |
| `--data-dir` | Katalog wyjściowy dla plików CSV          | `data/`  |

### `load`

```
python main.py load --volume {small|medium|large} [--database DB] [--data-dir DIR]
```

| Opcja         | Opis                                          | Domyślna |
|---------------|-----------------------------------------------|----------|
| `--volume`    | Rozmiar danych (musi zgadzać się z generate)  | wymagane |
| `--database`  | `postgres`, `mysql`, `mongo`, `neo4j`, `all`  | `all`    |
| `--data-dir`  | Katalog z plikami CSV                         | `data/`  |

---

## Dane dostępowe (domyślne)

| Baza danych | Host      | Port  | Użytkownik | Hasło     | Baza  |
|-------------|-----------|-------|------------|-----------|-------|
| PostgreSQL  | localhost | 5432  | vod        | vod123    | vod   |
| MySQL       | localhost | 3306  | vod        | vod123    | vod   |
| MongoDB     | localhost | 27017 | —          | —         | vod   |
| Neo4j       | localhost | 7687  | neo4j      | vod12345  | —     |

Panel webowy Neo4j dostępny pod: http://localhost:7474

---

## Struktura projektu

```
ZTBD-uni-project/
├── main.py                    # Punkt wejściowy (CLI)
├── requirements.txt           # Zależności Python
├── docker-compose.yml         # Konfiguracja kontenerów
├── data/
│   ├── small/                 # Wygenerowane pliki CSV (small)
│   └── medium/                # Wygenerowane pliki CSV (medium)
├── docker/
│   ├── postgres/init.sql      # Schemat PostgreSQL
│   └── mysql/init.sql         # Schemat MySQL
└── src/
    ├── config.py              # Konfiguracja wolumenów
    ├── generators/
    │   └── data_generator.py  # Generator danych (Faker)
    └── loaders/
        ├── postgres_loader.py # Ładowanie przez COPY
        ├── mysql_loader.py    # Ładowanie przez batch INSERT
        ├── mongo_loader.py    # Ładowanie dokumentów (denormalizacja)
        └── neo4j_loader.py    # Ładowanie grafu (węzły i relacje)
```
