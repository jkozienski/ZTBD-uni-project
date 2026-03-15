"""
Scenariusze benchmarkowe dla MongoDB (pymongo).

Struktura dokumentow w MongoDB (wg mongo_loader.py):
  users       — dokument z embedded profiles[] i subscription{}
  content     — dokument z embedded cast[] i seasons[{episodes[]}]
  watch_history — plaska kolekcja
  ratings     — plaska kolekcja
  payments    — plaska kolekcja
  my_list     — plaska kolekcja

conn w tym pliku to obiekt Database (pymongo), nie klient.
"""

import random
import uuid
from datetime import datetime, timedelta, timezone

from pymongo import DESCENDING


# ─────────────────────────────────────────────────────────────
# Pomocnicze funkcje
# ─────────────────────────────────────────────────────────────

def _rand_email():
    return f"test_{uuid.uuid4().hex[:12]}@benchmark.test"

def _rand_str(length=8):
    return uuid.uuid4().hex[:length]

def _now():
    return datetime.now(timezone.utc).isoformat()

def _get_random_profile_ids(db, limit=50):
    """Pobiera losowe profile_id z dokumentow users."""
    pipeline = [
        {"$unwind": "$profiles"},
        {"$project": {"_id": 0, "profile_id": "$profiles.profile_id"}},
        {"$sample": {"size": limit}},
    ]
    return [doc["profile_id"] for doc in db["users"].aggregate(pipeline)]

def _get_random_content_ids(db, limit=50):
    docs = list(db["content"].aggregate([{"$sample": {"size": limit}},
                                          {"$project": {"_id": 1}}]))
    return [d["_id"] for d in docs]

def _get_max_id(db, collection, id_field="_id"):
    """Pobiera maksymalne ID z kolekcji."""
    doc = db[collection].find_one(sort=[(id_field, DESCENDING)])
    return doc[id_field] if doc else 0


# ═════════════════════════════════════════════════════════════
# INSERT — 6 scenariuszy
# ═════════════════════════════════════════════════════════════

# ─── I1: Rejestracja uzytkownika ─────────────────────────────
# W MongoDB user to jeden dokument z embedded profiles i subscription

def setup_i1(db):
    # max_id poza pomiarem — to jest przygotowanie, nie testowana operacja
    max_id = _get_max_id(db, "users")
    return {"max_id": max_id}

def run_i1(db, ctx):
    user_id = ctx["max_id"] + random.randint(1, 1000)
    email = _rand_email()
    doc = {
        "_id": user_id,
        "email": email,
        "password_hash": "$2b$12$testhash",
        "first_name": "Jan",
        "last_name": "Testowy",
        "date_of_birth": "1990-01-01",
        "country_code": "PL",
        "phone": None,
        "status": "active",
        "created_at": _now(),
        "updated_at": _now(),
        "profiles": [
            {
                "profile_id": user_id * 10,
                "name": "Glowny",
                "is_kids": False,
                "maturity_rating": "ALL",
                "language": "pl",
                "created_at": _now(),
            }
        ],
        "subscription": {
            "subscription_id": user_id * 10,
            "plan_name": "basic",
            "price_monthly": 29.99,
            "max_streams": 1,
            "max_resolution": "HD",
            "status": "active",
            "start_date": datetime.now().strftime("%Y-%m-%d"),
            "end_date": None,
            "auto_renew": True,
        },
    }
    db["users"].insert_one(doc)
    ctx["user_id"] = user_id

def teardown_i1(db, ctx):
    db["users"].delete_one({"_id": ctx["user_id"]})


# ─── I2: Masowy import watch_history (1000 rekordow) ─────────

def setup_i2(db):
    max_id = _get_max_id(db, "watch_history")
    profile_ids = _get_random_profile_ids(db, 50)
    content_ids = _get_random_content_ids(db, 50)
    return {"max_id": max_id, "profile_ids": profile_ids, "content_ids": content_ids}

def run_i2(db, ctx):
    start_id = ctx["max_id"] + 1
    docs = [
        {
            "_id": start_id + i,
            "profile_id": random.choice(ctx["profile_ids"]),
            "content_id": random.choice(ctx["content_ids"]),
            "episode_id": None,
            "started_at": _now(),
            "progress_percent": round(random.uniform(0, 100), 2),
            "completed": False,
        }
        for i in range(1000)
    ]
    db["watch_history"].insert_many(docs, ordered=False)

def teardown_i2(db, ctx):
    db["watch_history"].delete_many({"_id": {"$gt": ctx["max_id"]}})


# ─── I3: Dodanie serialu z pelnym drzewem ────────────────────
# content z embedded cast i seasons

def setup_i3(db):
    # Pobierz kilka person_id z kolekcji content (sa embedded w cast[])
    sample = list(db["content"].aggregate([
        {"$unwind": "$cast"},
        {"$project": {"person_id": "$cast.person_id",
                       "first_name": "$cast.first_name",
                       "last_name": "$cast.last_name"}},
        {"$limit": 5},
    ]))
    return {"cast_sample": sample}

def run_i3(db, ctx):
    content_id = _get_max_id(db, "content") + random.randint(1, 1000)
    cast = [
        {
            "person_id": p["person_id"],
            "first_name": p["first_name"],
            "last_name": p["last_name"],
            "role": "actor",
            "character_name": None,
            "billing_order": idx + 1,
        }
        for idx, p in enumerate(ctx["cast_sample"][:3])
    ]
    seasons = [
        {
            "season_id": content_id * 10 + sn,
            "season_number": sn,
            "title": f"Sezon {sn}",
            "release_date": None,
            "episodes": [
                {
                    "episode_id": content_id * 100 + sn * 10 + ep,
                    "episode_number": ep,
                    "title": f"Odcinek {ep}",
                    "duration_minutes": 45,
                    "release_date": None,
                    "video_url": None,
                }
                for ep in range(1, 4)
            ],
        }
        for sn in range(1, 3)
    ]
    doc = {
        "_id": content_id,
        "title": f"Serial testowy {_rand_str()}",
        "type": "series",
        "maturity_rating": "ALL",
        "is_active": True,
        "avg_rating": 0.0,
        "total_views": 0,
        "popularity_score": 0.0,
        "cast": cast,
        "seasons": seasons,
        "created_at": _now(),
    }
    db["content"].insert_one(doc)
    ctx["content_id"] = content_id

def teardown_i3(db, ctx):
    db["content"].delete_one({"_id": ctx["content_id"]})


# ─── I4: Batch insert platnosci (1000 rekordow) ──────────────

def setup_i4(db):
    max_id = _get_max_id(db, "payments")
    # Pobierz subscription_id z dokumentow users
    sample = list(db["users"].aggregate([
        {"$match": {"subscription": {"$ne": None}}},
        {"$project": {"sub_id": "$subscription.subscription_id"}},
        {"$limit": 100},
    ]))
    sub_ids = [s["sub_id"] for s in sample if s.get("sub_id")]
    return {"max_id": max_id, "sub_ids": sub_ids}

def run_i4(db, ctx):
    if not ctx["sub_ids"]:
        raise RuntimeError("Brak subscription_id w bazie")
    methods = ["credit_card", "blik", "transfer", "paypal"]
    start_id = ctx["max_id"] + 1
    docs = [
        {
            "_id": start_id + i,
            "subscription_id": random.choice(ctx["sub_ids"]),
            "amount": round(random.uniform(19, 79), 2),
            "currency": "PLN",
            "payment_method": random.choice(methods),
            "transaction_id": f"TXN-{uuid.uuid4().hex[:16].upper()}",
            "status": "completed",
            "paid_at": _now(),
            "created_at": _now(),
        }
        for i in range(1000)
    ]
    db["payments"].insert_many(docs, ordered=False)

def teardown_i4(db, ctx):
    db["payments"].delete_many({"_id": {"$gt": ctx["max_id"]}})


# ─── I5: Dodanie oceny z przeliczeniem avg_rating ────────────

def setup_i5(db):
    max_id = _get_max_id(db, "ratings")
    # Znajdz pare profile_id + content_id bez oceny
    content_doc = db["content"].find_one()
    profile_ids = _get_random_profile_ids(db, 5)
    if not profile_ids:
        raise RuntimeError("Brak profili")
    content_id = content_doc["_id"]
    profile_id = profile_ids[0]
    old_avg = content_doc.get("avg_rating", 0.0)
    return {
        "max_id": max_id,
        "content_id": content_id,
        "profile_id": profile_id,
        "old_avg": old_avg,
    }

def run_i5(db, ctx):
    score = random.randint(1, 10)
    rating_id = ctx["max_id"] + 1
    db["ratings"].insert_one({
        "_id": rating_id,
        "profile_id": ctx["profile_id"],
        "content_id": ctx["content_id"],
        "score": score,
        "review_text": None,
        "created_at": _now(),
        "updated_at": _now(),
    })
    # Przelicz avg_rating
    pipeline = [
        {"$match": {"content_id": ctx["content_id"]}},
        {"$group": {"_id": None, "avg": {"$avg": "$score"}}},
    ]
    result = list(db["ratings"].aggregate(pipeline))
    new_avg = result[0]["avg"] if result else 0.0
    db["content"].update_one(
        {"_id": ctx["content_id"]},
        {"$set": {"avg_rating": round(new_avg, 2)}}
    )
    ctx["rating_id"] = rating_id

def teardown_i5(db, ctx):
    db["ratings"].delete_one({"_id": ctx["rating_id"]})
    db["content"].update_one(
        {"_id": ctx["content_id"]},
        {"$set": {"avg_rating": ctx["old_avg"]}}
    )


# ─── I6: Import osob z powiazaniami (100 osob) ───────────────
# W MongoDB osoby sa embedded w content.cast — dodajemy do losowych content

def setup_i6(db):
    content_ids = _get_random_content_ids(db, 100)
    return {"content_ids": content_ids, "added_person_ids": []}

def run_i6(db, ctx):
    # Generujemy 100 nowych "osob" (jako embedded cast entries w losowych content)
    nationalities = ["PL", "US", "GB", "DE", "FR"]
    fake_person_id_base = 9_000_000 + random.randint(0, 999_999)
    updates = []
    for i in range(100):
        person_id = fake_person_id_base + i
        content_id = random.choice(ctx["content_ids"])
        cast_entry = {
            "person_id": person_id,
            "first_name": f"Imie{i}",
            "last_name": f"Nazwisko{i}",
            "role": "actor",
            "character_name": None,
            "billing_order": i + 1,
        }
        updates.append((content_id, cast_entry))
        ctx["added_person_ids"].append((content_id, person_id))

    # Wykonaj updates (grupuj po content_id dla wydajnosci)
    by_content = {}
    for cid, entry in updates:
        by_content.setdefault(cid, []).append(entry)

    for cid, entries in by_content.items():
        db["content"].update_one(
            {"_id": cid},
            {"$push": {"cast": {"$each": entries}}}
        )

def teardown_i6(db, ctx):
    # Usun dodane osoby z cast (po person_id)
    by_content = {}
    for cid, pid in ctx["added_person_ids"]:
        by_content.setdefault(cid, []).append(pid)

    for cid, pids in by_content.items():
        db["content"].update_one(
            {"_id": cid},
            {"$pull": {"cast": {"person_id": {"$in": pids}}}}
        )


# ═════════════════════════════════════════════════════════════
# SELECT — 6 scenariuszy
# ═════════════════════════════════════════════════════════════

def setup_s1(db):
    return {}

def run_s1(db, ctx):
    list(db["content"].find(
        {"is_active": True},
        {"title": 1, "type": 1, "genres": 1, "avg_rating": 1, "popularity_score": 1},
    ).sort("popularity_score", DESCENDING).limit(20))

def teardown_s1(db, ctx):
    return


def setup_s2(db):
    # Znajdz profil z duza historia ogladania
    pipeline = [
        {"$group": {"_id": "$profile_id", "cnt": {"$sum": 1}}},
        {"$sort": {"cnt": DESCENDING}},
        {"$limit": 1},
    ]
    result = list(db["watch_history"].aggregate(pipeline))
    if not result:
        raise RuntimeError("Brak danych w watch_history")
    return {"profile_id": result[0]["_id"]}

def run_s2(db, ctx):
    pid = ctx["profile_id"]
    # Krok 1: treSci ogladane przez ten profil
    watched = [d["content_id"] for d in db["watch_history"].find(
        {"profile_id": pid}, {"content_id": 1}
    )]
    # Krok 2: inne profile ktore ogladaly to samo
    other_profiles_cursor = db["watch_history"].find(
        {"content_id": {"$in": watched}, "profile_id": {"$ne": pid}},
        {"profile_id": 1, "content_id": 1},
    )
    # Krok 3: zlicz treSci polecane
    counts = {}
    for doc in other_profiles_cursor:
        cid = doc["content_id"]
        if cid not in watched:
            counts[cid] = counts.get(cid, 0) + 1

    # Krok 4: posortuj i pobierz top 10
    top_cids = sorted(counts, key=counts.get, reverse=True)[:10]
    list(db["content"].find(
        {"_id": {"$in": top_cids}, "is_active": True},
        {"title": 1, "avg_rating": 1},
    ))

def teardown_s2(db, ctx):
    return


def setup_s3(db):
    return {}

def run_s3(db, ctx):
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    pipeline = [
        {"$match": {"started_at": {"$gte": cutoff.isoformat()}}},
        {"$group": {"_id": "$content_id", "views": {"$sum": 1}}},
        {"$sort": {"views": DESCENDING}},
        {"$limit": 100},
        {"$lookup": {
            "from": "content",
            "localField": "_id",
            "foreignField": "_id",
            "as": "content_info",
        }},
        {"$unwind": "$content_info"},
        {"$project": {"title": "$content_info.title", "views": 1}},
    ]
    list(db["watch_history"].aggregate(pipeline))

def teardown_s3(db, ctx):
    return


def setup_s4(db):
    return {}

def run_s4(db, ctx):
    keyword = random.choice(["the", "man", "love", "war", "world"])
    import re
    list(db["content"].find(
        {"title": {"$regex": keyword, "$options": "i"}, "is_active": True},
        {"title": 1, "type": 1, "avg_rating": 1},
    ).limit(20))

def teardown_s4(db, ctx):
    return


def setup_s5(db):
    pipeline = [
        {"$group": {"_id": "$profile_id", "cnt": {"$sum": 1}}},
        {"$sort": {"cnt": DESCENDING}},
        {"$limit": 1},
    ]
    result = list(db["watch_history"].aggregate(pipeline))
    if not result:
        raise RuntimeError("Brak danych w watch_history")
    return {"profile_id": result[0]["_id"]}

def run_s5(db, ctx):
    # Pobierz ostatnie 50 wpisow dla profilu
    history = list(db["watch_history"].find(
        {"profile_id": ctx["profile_id"]},
        {"content_id": 1, "started_at": 1, "progress_percent": 1, "completed": 1},
    ).sort("started_at", DESCENDING).limit(50))

    # Pobierz tytuly dla tych content_id
    content_ids = [h["content_id"] for h in history]
    list(db["content"].find(
        {"_id": {"$in": content_ids}},
        {"title": 1, "type": 1},
    ))

def teardown_s5(db, ctx):
    return


def setup_s6(db):
    # Znajdz osobe z najwieksza filmografia (embedded w cast[])
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast.person_id", "cnt": {"$sum": 1}}},
        {"$sort": {"cnt": DESCENDING}},
        {"$limit": 1},
    ]
    result = list(db["content"].aggregate(pipeline))
    if not result:
        raise RuntimeError("Brak danych w cast")
    return {"person_id": result[0]["_id"]}

def run_s6(db, ctx):
    pid = ctx["person_id"]
    list(db["content"].find(
        {"cast.person_id": pid},
        {"title": 1, "type": 1, "release_date": 1,
         "cast": {"$elemMatch": {"person_id": pid}}},
    ).sort("release_date", DESCENDING))

def teardown_s6(db, ctx):
    return


# ═════════════════════════════════════════════════════════════
# UPDATE — 6 scenariuszy
# ═════════════════════════════════════════════════════════════

def setup_u1(db):
    doc = db["watch_history"].find_one()
    if doc is None:
        raise RuntimeError("Brak danych w watch_history")
    return {"watch_id": doc["_id"], "old_progress": doc["progress_percent"]}

def run_u1(db, ctx):
    new_progress = round(random.uniform(10, 99), 2)
    db["watch_history"].update_one(
        {"_id": ctx["watch_id"]},
        {"$set": {"progress_percent": new_progress}}
    )

def teardown_u1(db, ctx):
    db["watch_history"].update_one(
        {"_id": ctx["watch_id"]},
        {"$set": {"progress_percent": ctx["old_progress"]}}
    )


def setup_u2(db):
    # Znajdz content ktory ma oceny
    rating_doc = db["ratings"].find_one()
    if rating_doc is None:
        raise RuntimeError("Brak danych w ratings")
    content_id = rating_doc["content_id"]
    content_doc = db["content"].find_one({"_id": content_id})
    return {"content_id": content_id, "old_avg": content_doc.get("avg_rating", 0.0)}

def run_u2(db, ctx):
    pipeline = [
        {"$match": {"content_id": ctx["content_id"]}},
        {"$group": {"_id": None, "avg": {"$avg": "$score"}}},
    ]
    result = list(db["ratings"].aggregate(pipeline))
    new_avg = result[0]["avg"] if result else 0.0
    db["content"].update_one(
        {"_id": ctx["content_id"]},
        {"$set": {"avg_rating": round(new_avg, 2)}}
    )

def teardown_u2(db, ctx):
    return


def setup_u3(db):
    # Pobierz 500 userow z aktywna subskrypcja
    docs = list(db["users"].find(
        {"subscription.status": "active"},
        {"_id": 1, "subscription.plan_name": 1},
    ).limit(500))
    if not docs:
        raise RuntimeError("Brak aktywnych subskrypcji")
    return {"originals": [(d["_id"], d["subscription"]["plan_name"]) for d in docs]}

def run_u3(db, ctx):
    user_ids = [r[0] for r in ctx["originals"]]
    db["users"].update_many(
        {"_id": {"$in": user_ids}},
        {"$set": {"subscription.plan_name": "premium"}}
    )

def teardown_u3(db, ctx):
    # Przywroc oryginalne plany (po jednym)
    for user_id, plan_name in ctx["originals"]:
        db["users"].update_one(
            {"_id": user_id},
            {"$set": {"subscription.plan_name": plan_name}}
        )


def setup_u4(db):
    doc = db["users"].find_one({}, {"email": 1, "phone": 1})
    return {"user_id": doc["_id"], "old_email": doc["email"], "old_phone": doc.get("phone")}

def run_u4(db, ctx):
    db["users"].update_one(
        {"_id": ctx["user_id"]},
        {"$set": {"email": _rand_email(), "phone": "+48 100 200 300", "updated_at": _now()}}
    )

def teardown_u4(db, ctx):
    db["users"].update_one(
        {"_id": ctx["user_id"]},
        {"$set": {"email": ctx["old_email"], "phone": ctx["old_phone"]}}
    )


def setup_u5(db):
    doc = db["content"].find_one({"is_active": True})
    if doc is None:
        raise RuntimeError("Brak aktywnych tresci")
    return {"content_id": doc["_id"]}

def run_u5(db, ctx):
    db["content"].update_one(
        {"_id": ctx["content_id"]},
        {"$set": {"is_active": False}}
    )

def teardown_u5(db, ctx):
    db["content"].update_one(
        {"_id": ctx["content_id"]},
        {"$set": {"is_active": True}}
    )


def setup_u6(db):
    return {}

def run_u6(db, ctx):
    # Przelicz popularity_score dla wszystkich aktywnych tresci
    # popularity_score = avg_rating * (total_views / 10000 + 1)
    pipeline = [
        {"$match": {"is_active": True}},
        {"$project": {
            "new_score": {
                "$multiply": [
                    "$avg_rating",
                    {"$add": [{"$divide": ["$total_views", 10000]}, 1]}
                ]
            }
        }},
    ]
    docs = list(db["content"].aggregate(pipeline))
    # Bulk update
    from pymongo import UpdateOne
    ops = [
        UpdateOne({"_id": d["_id"]}, {"$set": {"popularity_score": round(d["new_score"], 2)}})
        for d in docs
    ]
    if ops:
        db["content"].bulk_write(ops, ordered=False)

def teardown_u6(db, ctx):
    return


# ═════════════════════════════════════════════════════════════
# DELETE — 6 scenariuszy
# ═════════════════════════════════════════════════════════════

def setup_d1(db):
    # Wstaw testowy content z sezonami i historia
    content_id = _get_max_id(db, "content") + random.randint(1, 1000)
    doc = {
        "_id": content_id,
        "title": f"Do usuniecia {_rand_str()}",
        "type": "series",
        "maturity_rating": "ALL",
        "is_active": True,
        "avg_rating": 0.0,
        "total_views": 0,
        "popularity_score": 0.0,
        "cast": [],
        "seasons": [
            {
                "season_id": content_id * 10 + sn,
                "season_number": sn,
                "title": f"Sezon {sn}",
                "release_date": None,
                "episodes": [
                    {"episode_id": content_id * 100 + sn * 10 + ep,
                     "episode_number": ep, "title": f"Ep {ep}", "duration_minutes": 40}
                    for ep in range(1, 4)
                ],
            }
            for sn in range(1, 3)
        ],
        "created_at": _now(),
    }
    db["content"].insert_one(doc)

    # Dodaj kilka wpisow watch_history dla tego content
    profile_ids = _get_random_profile_ids(db, 5)
    max_wh = _get_max_id(db, "watch_history")
    wh_docs = [
        {"_id": max_wh + i + 1, "profile_id": pid, "content_id": content_id,
         "episode_id": None, "started_at": _now(), "progress_percent": 50.0, "completed": False}
        for i, pid in enumerate(profile_ids)
    ]
    if wh_docs:
        db["watch_history"].insert_many(wh_docs)

    return {"content_id": content_id}

def run_d1(db, ctx):
    db["content"].delete_one({"_id": ctx["content_id"]})
    # W MongoDB watch_history to osobna kolekcja — brak automatycznego CASCADE
    db["watch_history"].delete_many({"content_id": ctx["content_id"]})
    db["ratings"].delete_many({"content_id": ctx["content_id"]})
    db["my_list"].delete_many({"content_id": ctx["content_id"]})

def teardown_d1(db, ctx):
    return


def setup_d2(db):
    # Wstaw testowego usera z profilem
    user_id = _get_max_id(db, "users") + random.randint(1, 1000)
    profile_id = user_id * 10
    db["users"].insert_one({
        "_id": user_id,
        "email": _rand_email(),
        "password_hash": "hash",
        "first_name": "Test",
        "last_name": "D2",
        "date_of_birth": "1990-01-01",
        "country_code": "PL",
        "status": "active",
        "created_at": _now(),
        "updated_at": _now(),
        "profiles": [{"profile_id": profile_id, "name": "Profil D2",
                       "is_kids": False, "maturity_rating": "ALL", "language": "pl",
                       "created_at": _now()}],
        "subscription": None,
    })
    # Dodaj watch_history dla tego profilu
    content_ids = _get_random_content_ids(db, 5)
    max_wh = _get_max_id(db, "watch_history")
    wh_docs = [
        {"_id": max_wh + i + 1, "profile_id": profile_id, "content_id": cid,
         "episode_id": None, "started_at": _now(), "progress_percent": 30.0, "completed": False}
        for i, cid in enumerate(content_ids)
    ]
    if wh_docs:
        db["watch_history"].insert_many(wh_docs)

    return {"user_id": user_id, "profile_id": profile_id}

def run_d2(db, ctx):
    # Usun profil z embedded tablicy
    db["users"].update_one(
        {"_id": ctx["user_id"]},
        {"$pull": {"profiles": {"profile_id": ctx["profile_id"]}}}
    )
    # Usun powiazane dane (brak CASCADE w Mongo)
    db["watch_history"].delete_many({"profile_id": ctx["profile_id"]})
    db["ratings"].delete_many({"profile_id": ctx["profile_id"]})
    db["my_list"].delete_many({"profile_id": ctx["profile_id"]})

def teardown_d2(db, ctx):
    db["users"].delete_one({"_id": ctx["user_id"]})


def setup_d3(db):
    max_id = _get_max_id(db, "watch_history")
    profile_ids = _get_random_profile_ids(db, 10)
    content_ids = _get_random_content_ids(db, 20)

    old_date = (datetime.now(timezone.utc) - timedelta(days=800)).isoformat()
    docs = [
        {
            "_id": max_id + i + 1,
            "profile_id": random.choice(profile_ids),
            "content_id": random.choice(content_ids),
            "episode_id": None,
            "started_at": old_date,
            "progress_percent": 0.0,
            "completed": False,
        }
        for i in range(100)
    ]
    db["watch_history"].insert_many(docs, ordered=False)
    return {"max_id": max_id}

def run_d3(db, ctx):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=365)).isoformat()
    db["watch_history"].delete_many({
        "started_at": {"$lt": cutoff},
        "_id": {"$gt": ctx["max_id"]},
    })

def teardown_d3(db, ctx):
    return


def setup_d4(db):
    profile_ids = _get_random_profile_ids(db, 1)
    if not profile_ids:
        raise RuntimeError("Brak profili")
    profile_id = profile_ids[0]

    content_ids = _get_random_content_ids(db, 1)
    if not content_ids:
        raise RuntimeError("Brak contentu")
    content_id = content_ids[0]

    max_id = _get_max_id(db, "my_list")
    db["my_list"].insert_one({
        "_id": max_id + 1,
        "profile_id": profile_id,
        "content_id": content_id,
        "added_at": _now(),
        "sort_order": 0,
    })
    return {"profile_id": profile_id, "content_id": content_id}

def run_d4(db, ctx):
    db["my_list"].delete_one({
        "profile_id": ctx["profile_id"],
        "content_id": ctx["content_id"],
    })

def teardown_d4(db, ctx):
    return


def setup_d5(db):
    user_id = _get_max_id(db, "users") + random.randint(1, 1000)
    sub_id = user_id * 10
    db["users"].insert_one({
        "_id": user_id,
        "email": _rand_email(),
        "password_hash": "hash",
        "first_name": "Test",
        "last_name": "D5",
        "date_of_birth": "1990-01-01",
        "country_code": "PL",
        "status": "active",
        "created_at": _now(),
        "updated_at": _now(),
        "profiles": [],
        "subscription": {
            "subscription_id": sub_id,
            "plan_name": "basic",
            "price_monthly": 29.99,
            "status": "active",
        },
    })
    # Dodaj 5 platnosci
    max_pay = _get_max_id(db, "payments")
    pay_docs = [
        {"_id": max_pay + i + 1, "subscription_id": sub_id,
         "amount": 29.99, "currency": "PLN", "payment_method": "blik",
         "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
         "status": "completed", "paid_at": _now(), "created_at": _now()}
        for i in range(5)
    ]
    db["payments"].insert_many(pay_docs)
    return {"user_id": user_id, "sub_id": sub_id}

def run_d5(db, ctx):
    # Usun subskrypcje (embed w userze) i powiazane platnosci
    db["users"].update_one(
        {"_id": ctx["user_id"]},
        {"$set": {"subscription": None}}
    )
    db["payments"].delete_many({"subscription_id": ctx["sub_id"]})

def teardown_d5(db, ctx):
    db["users"].delete_one({"_id": ctx["user_id"]})


def setup_d6(db):
    max_id = _get_max_id(db, "users")
    docs = [
        {
            "_id": max_id + i + 1,
            "email": _rand_email(),
            "password_hash": "hash",
            "first_name": "Del",
            "last_name": f"User{i}",
            "date_of_birth": "1980-01-01",
            "country_code": "PL",
            "status": "deleted",
            "created_at": _now(),
            "updated_at": _now(),
            "profiles": [],
            "subscription": None,
        }
        for i in range(50)
    ]
    db["users"].insert_many(docs, ordered=False)
    return {"max_id": max_id}

def run_d6(db, ctx):
    db["users"].delete_many({
        "status": "deleted",
        "_id": {"$gt": ctx["max_id"]},
    })

def teardown_d6(db, ctx):
    return


# ═════════════════════════════════════════════════════════════
# Lista wszystkich scenariuszy (uzywana przez runner.py)
# ═════════════════════════════════════════════════════════════

SCENARIOS = [
    {"id": "I1", "name": "Rejestracja uzytkownika (embedded doc)",
     "setup": setup_i1, "run": run_i1, "teardown": teardown_i1},
    {"id": "I2", "name": "Batch insert watch_history (1000 rekordow)",
     "setup": setup_i2, "run": run_i2, "teardown": teardown_i2},
    {"id": "I3", "name": "Dodanie serialu z drzewem (embedded cast+seasons)",
     "setup": setup_i3, "run": run_i3, "teardown": teardown_i3},
    {"id": "I4", "name": "Batch insert platnosci (1000 rekordow)",
     "setup": setup_i4, "run": run_i4, "teardown": teardown_i4},
    {"id": "I5", "name": "Dodanie oceny z przeliczeniem avg_rating",
     "setup": setup_i5, "run": run_i5, "teardown": teardown_i5},
    {"id": "I6", "name": "Import osob z powiazaniami (100 embedded cast)",
     "setup": setup_i6, "run": run_i6, "teardown": teardown_i6},

    {"id": "S1", "name": "Strona glowna (filtrowanie + sortowanie)",
     "setup": setup_s1, "run": run_s1, "teardown": teardown_s1},
    {"id": "S2", "name": "Rekomendacje collaborative filtering",
     "setup": setup_s2, "run": run_s2, "teardown": teardown_s2},
    {"id": "S3", "name": "TOP 100 tresci wg ogladalnosci (ostatni miesiac)",
     "setup": setup_s3, "run": run_s3, "teardown": teardown_s3},
    {"id": "S4", "name": "Wyszukiwanie po tytule (regex ILIKE)",
     "setup": setup_s4, "run": run_s4, "teardown": teardown_s4},
    {"id": "S5", "name": "Historia ogladania profilu (50 ostatnich z JOIN)",
     "setup": setup_s5, "run": run_s5, "teardown": teardown_s5},
    {"id": "S6", "name": "Filmografia osoby (embedded cast lookup)",
     "setup": setup_s6, "run": run_s6, "teardown": teardown_s6},

    {"id": "U1", "name": "Aktualizacja postepu ogladania",
     "setup": setup_u1, "run": run_u1, "teardown": teardown_u1},
    {"id": "U2", "name": "Przeliczenie avg_rating (aggregation pipeline)",
     "setup": setup_u2, "run": run_u2, "teardown": teardown_u2},
    {"id": "U3", "name": "Masowa zmiana planu subskrypcji (500 dokumentow)",
     "setup": setup_u3, "run": run_u3, "teardown": teardown_u3},
    {"id": "U4", "name": "Aktualizacja danych uzytkownika (email+phone)",
     "setup": setup_u4, "run": run_u4, "teardown": teardown_u4},
    {"id": "U5", "name": "Oznaczenie tresci jako nieaktywnej",
     "setup": setup_u5, "run": run_u5, "teardown": teardown_u5},
    {"id": "U6", "name": "Masowa aktualizacja popularity_score (bulk_write)",
     "setup": setup_u6, "run": run_u6, "teardown": teardown_u6},

    {"id": "D1", "name": "Usuniecie tresci (brak CASCADE — reczne czyszczenie)",
     "setup": setup_d1, "run": run_d1, "teardown": teardown_d1},
    {"id": "D2", "name": "Usuniecie profilu z historia (pull + delete_many)",
     "setup": setup_d2, "run": run_d2, "teardown": teardown_d2},
    {"id": "D3", "name": "Czyszczenie starej historii ogladania",
     "setup": setup_d3, "run": run_d3, "teardown": teardown_d3},
    {"id": "D4", "name": "Usuniecie pozycji z my_list",
     "setup": setup_d4, "run": run_d4, "teardown": teardown_d4},
    {"id": "D5", "name": "Usuniecie subskrypcji z platnosciami",
     "setup": setup_d5, "run": run_d5, "teardown": teardown_d5},
    {"id": "D6", "name": "Masowe usuniecie uzytkownikow status=deleted",
     "setup": setup_d6, "run": run_d6, "teardown": teardown_d6},
]

# Automatycznie uzupelnij pole "category" na podstawie prefiksu id
_CATEGORY_MAP = {"I": "INSERT", "S": "SELECT", "U": "UPDATE", "D": "DELETE"}
for _s in SCENARIOS:
    _s["category"] = _CATEGORY_MAP[_s["id"][0]]
