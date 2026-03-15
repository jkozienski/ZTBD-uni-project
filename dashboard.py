#!/usr/bin/env python3
"""
Dashboard do analizy wynikow benchmarkow ZTBD.

Uruchomienie:
  pip install streamlit pandas plotly
  streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path


# ── Konfiguracja strony ────────────────────────────────────────────────────────

st.set_page_config(
    page_title="ZTBD Benchmark Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("📊 ZTBD — Analiza benchmarków CRUD")
st.caption("Porównanie PostgreSQL, MySQL, MongoDB, Neo4j")


# ── Sidebar: wybór pliku i filtry ─────────────────────────────────────────────

with st.sidebar:
    st.header("Ustawienia")

    # Znajdź pliki CSV w katalogu results/
    results_dir = Path("results")
    csv_files = sorted(results_dir.glob("*.csv"), reverse=True)  # najnowsze pierwsze

    if not csv_files:
        st.error("Brak plików CSV w katalogu results/. Uruchom najpierw benchmark.")
        st.stop()

    csv_names = [f.name for f in csv_files]
    selected_file = st.selectbox("Plik wynikowy", csv_names)
    csv_path = results_dir / selected_file

    st.divider()

    # Wczytaj dane
    df = pd.read_csv(csv_path)

    # Filtr kategorii
    all_categories = ["INSERT", "SELECT", "UPDATE", "DELETE"]
    selected_categories = st.multiselect(
        "Kategoria operacji",
        options=all_categories,
        default=all_categories,
    )

    # Filtr baz danych
    all_databases = sorted(df["database"].unique())
    selected_databases = st.multiselect(
        "Baza danych",
        options=all_databases,
        default=all_databases,
    )

    st.divider()
    st.markdown(f"**Plik:** `{selected_file}`")
    st.markdown(f"**Pomiary łącznie:** {len(df)}")


# ── Filtrowanie danych ────────────────────────────────────────────────────────

if not selected_categories or not selected_databases:
    st.warning("Wybierz przynajmniej jedną kategorię i jedną bazę danych.")
    st.stop()

mask = df["category"].isin(selected_categories) & df["database"].isin(selected_databases)
filtered = df[mask].copy()

if filtered.empty:
    st.warning("Brak danych dla wybranych filtrów.")
    st.stop()

# Sortowanie scenariuszy w naturalnej kolejności (I1, I2, ..., S1, ..., D6)
category_order = {"INSERT": 0, "SELECT": 1, "UPDATE": 2, "DELETE": 3}
filtered["cat_order"] = filtered["category"].map(category_order)
filtered = filtered.sort_values(["cat_order", "scenario_id"]).drop(columns="cat_order")


# ── Sekcja 1: Tabela średnich ─────────────────────────────────────────────────

st.subheader("Tabela średnich czasów [ms]")
st.caption("Średnia z wszystkich prób (trials) dla każdego scenariusza.")

# Średnia per (scenario_id, database) — bez name, bo różni się między bazami
avg = (
    filtered
    .groupby(["scenario_id", "database"], sort=False)["time_ms"]
    .mean()
    .reset_index()
    .rename(columns={"time_ms": "avg_ms"})
)

# Metadane: kategoria + pierwsza dostępna nazwa dla każdego scenario_id
meta = (
    filtered
    .groupby("scenario_id")[["category", "name"]]
    .first()
    .reset_index()
)
avg = avg.merge(meta, on="scenario_id")

pivot = avg.pivot(index="scenario_id", columns="database", values="avg_ms")
pivot.columns.name = None
pivot = pivot.reset_index()
pivot = pivot.merge(meta, on="scenario_id")

# Kolejność kolumn: ID, Scenariusz, Kategoria, bazy
db_cols = [c for c in pivot.columns if c in all_databases]
pivot = pivot[["scenario_id", "name", "category"] + db_cols]
pivot = pivot.rename(columns={"scenario_id": "ID", "name": "Scenariusz", "category": "Kategoria"})

# Formatowanie + kolory czytelne na ciemnym tle
styled = pivot.style.format(
    {col: "{:.2f}" for col in db_cols},
    na_rep="—",
).highlight_min(
    subset=db_cols,
    axis=1,
    props="background-color:#1a6e35; color:white; font-weight:bold;",  # ciemna zieleń
).highlight_max(
    subset=db_cols,
    axis=1,
    props="background-color:#8b1a1a; color:white; font-weight:bold;",  # ciemna czerwień
)

st.dataframe(styled, use_container_width=True, hide_index=True)
st.caption("🟢 Ciemna zieleń = najszybszy  |  🔴 Ciemna czerwień = najwolniejszy  w danym wierszu")


# ── Sekcja 2: Wykres słupkowy ─────────────────────────────────────────────────

st.subheader("Porównanie średnich czasów [ms]")
st.caption("Grouped bar chart — każda grupa słupków to jeden scenariusz, kolory = bazy danych.")

# Zbuduj etykietę: "I1: Rejestracja..."  (skróć nazwę jeśli za długa)
avg["label"] = avg["scenario_id"] + ": " + avg["name"].str[:35]

# Kolejność scenariuszy na osi X
scenario_order = avg.drop_duplicates("scenario_id")["label"].tolist()

fig_bar = px.bar(
    avg,
    x="label",
    y="avg_ms",
    color="database",
    barmode="group",
    category_orders={"label": scenario_order},
    labels={"avg_ms": "Średni czas [ms]", "label": "Scenariusz", "database": "Baza"},
    title="Średni czas wykonania [ms] — wszystkie scenariusze",
    color_discrete_map={
        "postgres": "#336791",
        "mysql":    "#e48e00",
        "mongo":    "#4caf50",
        "neo4j":    "#e74c3c",
    },
    hover_data={"name": True, "category": True},
)
fig_bar.update_layout(xaxis_tickangle=-40, height=480)
st.plotly_chart(fig_bar, use_container_width=True)


# ── Sekcja 3: Wykres pudełkowy ────────────────────────────────────────────────

st.subheader("Rozrzut wyników między próbami [ms]")
st.caption(
    "Box plot pokazuje minimalny, maksymalny i medianowy czas z 3 prób. "
    "Duże pudełko = niespójna baza."
)

# Dodaj etykietę do filtered też
filtered["label"] = filtered["scenario_id"] + ": " + filtered["name"].str[:35]
scenario_order_box = (
    filtered.drop_duplicates("scenario_id")[["scenario_id", "label"]]
    .sort_values("scenario_id")["label"]
    .tolist()
)

fig_box = px.box(
    filtered,
    x="label",
    y="time_ms",
    color="database",
    category_orders={"label": scenario_order_box},
    labels={"time_ms": "Czas [ms]", "label": "Scenariusz", "database": "Baza"},
    title="Rozrzut czasów [ms] — zmienność między próbami",
    color_discrete_map={
        "postgres": "#336791",
        "mysql":    "#e48e00",
        "mongo":    "#4caf50",
        "neo4j":    "#e74c3c",
    },
    hover_data=["trial"],
    points="all",  # pokaż też punkty poszczególnych prób
)
fig_box.update_layout(xaxis_tickangle=-40, height=500)
st.plotly_chart(fig_box, use_container_width=True)


# ── Stopka ────────────────────────────────────────────────────────────────────

st.divider()
st.caption("ZTBD — Zaawansowane Technologie Baz Danych | Projekt VOD")
