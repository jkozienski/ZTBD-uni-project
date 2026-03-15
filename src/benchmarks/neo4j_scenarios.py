"""
Scenariusze benchmarkowe dla Neo4j (neo4j Python driver).

conn w tym pliku to obiekt Driver (GraphDatabase.driver).
Kazde zapytanie wykonujemy przez session.run().

Wezly i relacje (wg neo4j_loader.py):
  User, Profile, Subscription, Payment
  Content, Season, Episode, Person, Genre
  Relacje: HAS_PROFILE, HAS_SUBSCRIPTION, HAS_PAYMENT,
           HAS_SEASON, HAS_EPISODE, HAS_GENRE,
           ACTED_IN, DIRECTED, WROTE,
           WATCHED, RATED, ADDED_TO_LIST
"""

import random
import uuid
from datetime import datetime, timedelta


# ─────────────────────────────────────────────────────────────
# Pomocnicze funkcje
# ─────────────────────────────────────────────────────────────

def _rand_email():
    return f"test_{uuid.uuid4().hex[:12]}@benchmark.test"

def _rand_str(length=8):
    return uuid.uuid4().hex[:length]

def _run(driver, query, params=None):
    """Pomocnik — uruchamia zapytanie Cypher i zwraca liste rekordow."""
    with driver.session() as session:
        result = session.run(query, params or {})
        return result.data()

def _run_write(driver, query, params=None):
    """Pomocnik — uruchamia zapytanie zapisujace."""
    with driver.session() as session:
        session.run(query, params or {})

def _get_random_profile_ids(driver, limit=10):
    rows = _run(driver, "MATCH (p:Profile) RETURN p.profile_id AS pid ORDER BY rand() LIMIT $n", {"n": limit})
    return [r["pid"] for r in rows]

def _get_random_content_ids(driver, limit=10):
    rows = _run(driver, "MATCH (c:Content) RETURN c.content_id AS cid ORDER BY rand() LIMIT $n", {"n": limit})
    return [r["cid"] for r in rows]

def _get_max_user_id(driver):
    rows = _run(driver, "MATCH (u:User) RETURN max(u.user_id) AS m")
    return rows[0]["m"] or 0


# ═════════════════════════════════════════════════════════════
# INSERT — 6 scenariuszy
# ═════════════════════════════════════════════════════════════

# ─── I1: Rejestracja uzytkownika ─────────────────────────────

def setup_i1(driver):
    # max_id poza pomiarem — to jest przygotowanie, nie testowana operacja
    max_id = _get_max_user_id(driver)
    return {"max_id": max_id}

def run_i1(driver, ctx):
    user_id = ctx["max_id"] + random.randint(1, 1000)
    email = _rand_email()
    profile_id = user_id * 10
    sub_id = user_id * 10 + 1

    _run_write(driver, """
        CREATE (u:User {user_id: $uid, email: $email,
                        first_name: 'Jan', last_name: 'Testowy',
                        country_code: 'PL', status: 'active',
                        created_at: $now})
        CREATE (p:Profile {profile_id: $pid, name: 'Glowny',
                           is_kids: false, maturity_rating: 'ALL', language: 'pl'})
        CREATE (s:Subscription {subscription_id: $sid, plan_name: 'basic',
                                price_monthly: 29.99, max_streams: 1,
                                max_resolution: 'HD', status: 'active',
                                start_date: $today, auto_renew: true})
        CREATE (u)-[:HAS_PROFILE]->(p)
        CREATE (u)-[:HAS_SUBSCRIPTION]->(s)
    """, {
        "uid": user_id, "email": email, "pid": profile_id,
        "sid": sub_id, "now": datetime.now().isoformat(),
        "today": datetime.now().strftime("%Y-%m-%d"),
    })
    ctx["user_id"] = user_id
    ctx["profile_id"] = profile_id
    ctx["sub_id"] = sub_id

def teardown_i1(driver, ctx):
    _run_write(driver, """
        MATCH (u:User {user_id: $uid})
        OPTIONAL MATCH (u)-[:HAS_PROFILE]->(p:Profile)
        OPTIONAL MATCH (u)-[:HAS_SUBSCRIPTION]->(s:Subscription)
        DETACH DELETE u, p, s
    """, {"uid": ctx["user_id"]})


# ─── I2: Masowy import relacji WATCHED (1000 par) ────────────

def setup_i2(driver):
    profile_ids = _get_random_profile_ids(driver, 20)
    content_ids = _get_random_content_ids(driver, 20)
    return {"profile_ids": profile_ids, "content_ids": content_ids}

def run_i2(driver, ctx):
    rows = [
        {
            "pid": random.choice(ctx["profile_ids"]),
            "cid": random.choice(ctx["content_ids"]),
            "started_at": datetime.now().isoformat(),
            "progress": round(random.uniform(0, 100), 2),
            "completed": False,
        }
        for _ in range(1000)
    ]
    with driver.session() as session:
        # Neo4j: batch po 100 relacji naraz
        batch_size = 100
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            session.run("""
                UNWIND $rows AS r
                MATCH (p:Profile {profile_id: r.pid})
                MATCH (c:Content {content_id: r.cid})
                CREATE (p)-[:WATCHED {started_at: r.started_at,
                                       progress_percent: r.progress,
                                       completed: r.completed}]->(c)
            """, {"rows": batch})

def teardown_i2(driver, ctx):
    # Usun tylko relacje WATCHED ktore wskazuja na content z naszej listy
    # (nie mozemy latwo zidentyfikowac "nasze" — usun wszystkie dodane w czasie testu)
    # Zamiast tego — teardown jest pusty (akceptujemy dodatkowe relacje WATCHED)
    # W prawdziwym projekcie mozna dodac timestamp i usuwac po nim
    return


# ─── I3: Dodanie serialu z pelnym drzewem ────────────────────

def setup_i3(driver):
    rows = _run(driver, "MATCH (p:Person) RETURN p.person_id AS pid LIMIT 5")
    return {"person_ids": [r["pid"] for r in rows]}

def run_i3(driver, ctx):
    content_id = random.randint(9_000_000, 9_999_999)
    _run_write(driver, """
        CREATE (c:Content {content_id: $cid, title: $title, type: 'series',
                           maturity_rating: 'ALL', is_active: true,
                           avg_rating: 0.0, total_views: 0, popularity_score: 0.0,
                           created_at: $now})
    """, {"cid": content_id, "title": f"Serial testowy {_rand_str()}",
          "now": datetime.now().isoformat()})

    # 2 sezony
    for sn in range(1, 3):
        season_id = content_id * 10 + sn
        _run_write(driver, """
            MATCH (c:Content {content_id: $cid})
            CREATE (s:Season {season_id: $sid, season_number: $sn, title: $title})
            CREATE (c)-[:HAS_SEASON]->(s)
        """, {"cid": content_id, "sid": season_id, "sn": sn, "title": f"Sezon {sn}"})

        for ep in range(1, 4):
            ep_id = season_id * 10 + ep
            _run_write(driver, """
                MATCH (s:Season {season_id: $sid})
                CREATE (e:Episode {episode_id: $eid, episode_number: $en,
                                   title: $title, duration_minutes: 45})
                CREATE (s)-[:HAS_EPISODE]->(e)
            """, {"sid": season_id, "eid": ep_id, "en": ep, "title": f"Odcinek {ep}"})

    # Obsada
    for pid in ctx["person_ids"][:3]:
        _run_write(driver, """
            MATCH (p:Person {person_id: $pid})
            MATCH (c:Content {content_id: $cid})
            MERGE (p)-[:ACTED_IN]->(c)
        """, {"pid": pid, "cid": content_id})

    ctx["content_id"] = content_id

def teardown_i3(driver, ctx):
    _run_write(driver, """
        MATCH (c:Content {content_id: $cid})
        OPTIONAL MATCH (c)-[:HAS_SEASON]->(s:Season)
        OPTIONAL MATCH (s)-[:HAS_EPISODE]->(e:Episode)
        DETACH DELETE c, s, e
    """, {"cid": ctx["content_id"]})


# ─── I4: Batch insert platnosci (wezly Payment) ──────────────

def setup_i4(driver):
    rows = _run(driver, "MATCH (s:Subscription) RETURN s.subscription_id AS sid LIMIT 100")
    sub_ids = [r["sid"] for r in rows]
    rows2 = _run(driver, "MATCH (p:Payment) RETURN max(p.payment_id) AS m")
    max_id = rows2[0]["m"] or 0
    return {"sub_ids": sub_ids, "max_id": max_id}

def run_i4(driver, ctx):
    methods = ["credit_card", "blik", "transfer", "paypal"]
    batch = [
        {
            "pid": ctx["max_id"] + i + 1,
            "sid": random.choice(ctx["sub_ids"]),
            "amount": round(random.uniform(19, 79), 2),
            "method": random.choice(methods),
            "txn": f"TXN-{uuid.uuid4().hex[:12].upper()}",
        }
        for i in range(1000)
    ]
    with driver.session() as session:
        chunk_size = 100
        for i in range(0, len(batch), chunk_size):
            chunk = batch[i:i + chunk_size]
            session.run("""
                UNWIND $rows AS r
                MATCH (s:Subscription {subscription_id: r.sid})
                CREATE (pay:Payment {payment_id: r.pid, amount: r.amount,
                                     currency: 'PLN', payment_method: r.method,
                                     transaction_id: r.txn, status: 'completed'})
                CREATE (s)-[:HAS_PAYMENT]->(pay)
            """, {"rows": chunk})
    ctx["inserted_ids"] = [b["pid"] for b in batch]

def teardown_i4(driver, ctx):
    _run_write(driver, """
        UNWIND $ids AS pid
        MATCH (p:Payment {payment_id: pid})
        DETACH DELETE p
    """, {"ids": ctx.get("inserted_ids", [])})


# ─── I5: Dodanie oceny (relacja RATED) z przeliczeniem avg ───

def setup_i5(driver):
    rows = _run(driver, "MATCH (p:Profile) RETURN p.profile_id AS pid LIMIT 1")
    rows2 = _run(driver, "MATCH (c:Content) RETURN c.content_id AS cid, c.avg_rating AS avg LIMIT 1")
    if not rows or not rows2:
        raise RuntimeError("Brak profili lub contentu")
    return {
        "profile_id": rows[0]["pid"],
        "content_id": rows2[0]["cid"],
        "old_avg": rows2[0]["avg"],
    }

def run_i5(driver, ctx):
    score = random.randint(1, 10)
    _run_write(driver, """
        MATCH (p:Profile {profile_id: $pid})
        MATCH (c:Content {content_id: $cid})
        MERGE (p)-[r:RATED]->(c)
        SET r.score = $score, r.created_at = $now
    """, {"pid": ctx["profile_id"], "cid": ctx["content_id"],
          "score": score, "now": datetime.now().isoformat()})

    # Przelicz avg_rating
    rows = _run(driver, """
        MATCH ()-[r:RATED]->(c:Content {content_id: $cid})
        RETURN avg(r.score) AS avg_score
    """, {"cid": ctx["content_id"]})
    new_avg = rows[0]["avg_score"] if rows else 0.0

    _run_write(driver, """
        MATCH (c:Content {content_id: $cid})
        SET c.avg_rating = $avg
    """, {"cid": ctx["content_id"], "avg": round(new_avg or 0.0, 2)})

def teardown_i5(driver, ctx):
    _run_write(driver, """
        MATCH (p:Profile {profile_id: $pid})-[r:RATED]->(c:Content {content_id: $cid})
        DELETE r
    """, {"pid": ctx["profile_id"], "cid": ctx["content_id"]})
    _run_write(driver, """
        MATCH (c:Content {content_id: $cid}) SET c.avg_rating = $avg
    """, {"cid": ctx["content_id"], "avg": ctx["old_avg"]})


# ─── I6: Import osob z powiazaniami (100 Person + ACTED_IN) ──

def setup_i6(driver):
    content_ids = _get_random_content_ids(driver, 20)
    rows = _run(driver, "MATCH (p:Person) RETURN max(p.person_id) AS m")
    max_id = rows[0]["m"] or 0
    return {"content_ids": content_ids, "max_id": max_id}

def run_i6(driver, ctx):
    nationalities = ["PL", "US", "GB", "DE", "FR"]
    people = [
        {
            "pid": ctx["max_id"] + i + 1,
            "first_name": f"Imie{i}",
            "last_name": f"Nazwisko{i}",
            "nationality": random.choice(nationalities),
            "cid": random.choice(ctx["content_ids"]),
        }
        for i in range(100)
    ]
    with driver.session() as session:
        chunk_size = 50
        for i in range(0, len(people), chunk_size):
            chunk = people[i:i + chunk_size]
            session.run("""
                UNWIND $rows AS r
                CREATE (p:Person {person_id: r.pid,
                                   first_name: r.first_name,
                                   last_name: r.last_name,
                                   nationality: r.nationality})
                WITH p, r
                MATCH (c:Content {content_id: r.cid})
                CREATE (p)-[:ACTED_IN]->(c)
            """, {"rows": chunk})
    ctx["inserted_ids"] = [p["pid"] for p in people]

def teardown_i6(driver, ctx):
    _run_write(driver, """
        UNWIND $ids AS pid
        MATCH (p:Person {person_id: pid})
        DETACH DELETE p
    """, {"ids": ctx.get("inserted_ids", [])})


# ═════════════════════════════════════════════════════════════
# SELECT — 6 scenariuszy
# ═════════════════════════════════════════════════════════════

def setup_s1(driver):
    return {}

def run_s1(driver, ctx):
    _run(driver, """
        MATCH (c:Content)
        WHERE c.is_active = true
        RETURN c.content_id, c.title, c.type, c.avg_rating, c.popularity_score
        ORDER BY c.popularity_score DESC
        LIMIT 20
    """)

def teardown_s1(driver, ctx):
    return


def setup_s2(driver):
    rows = _run(driver, """
        MATCH (p:Profile)-[:WATCHED]->(c:Content)
        WITH p, count(c) AS cnt
        ORDER BY cnt DESC LIMIT 1
        RETURN p.profile_id AS pid
    """)
    if not rows:
        raise RuntimeError("Brak danych WATCHED")
    return {"profile_id": rows[0]["pid"]}

def run_s2(driver, ctx):
    _run(driver, """
        MATCH (me:Profile {profile_id: $pid})-[:WATCHED]->(c:Content)
        WITH me, collect(c) AS my_content
        MATCH (other:Profile)-[:WATCHED]->(c2:Content)
        WHERE other <> me AND c2 IN my_content
        WITH other, my_content
        MATCH (other)-[:WATCHED]->(rec:Content)
        WHERE NOT rec IN my_content AND rec.is_active = true
        RETURN rec.content_id, rec.title, rec.avg_rating, count(*) AS score
        ORDER BY score DESC LIMIT 10
    """, {"pid": ctx["profile_id"]})

def teardown_s2(driver, ctx):
    return


def setup_s3(driver):
    return {}

def run_s3(driver, ctx):
    cutoff = (datetime.now() - timedelta(days=30)).isoformat()
    _run(driver, """
        MATCH (p:Profile)-[w:WATCHED]->(c:Content)
        WHERE w.started_at >= $cutoff
        RETURN c.content_id, c.title, count(w) AS views
        ORDER BY views DESC LIMIT 100
    """, {"cutoff": cutoff})

def teardown_s3(driver, ctx):
    return


def setup_s4(driver):
    return {}

def run_s4(driver, ctx):
    keyword = random.choice(["the", "man", "love", "war", "world"])
    _run(driver, """
        MATCH (c:Content)
        WHERE c.title CONTAINS $kw AND c.is_active = true
        RETURN c.content_id, c.title, c.type, c.avg_rating
        LIMIT 20
    """, {"kw": keyword})

def teardown_s4(driver, ctx):
    return


def setup_s5(driver):
    rows = _run(driver, """
        MATCH (p:Profile)-[:WATCHED]->(c:Content)
        WITH p, count(c) AS cnt ORDER BY cnt DESC LIMIT 1
        RETURN p.profile_id AS pid
    """)
    if not rows:
        raise RuntimeError("Brak danych WATCHED")
    return {"profile_id": rows[0]["pid"]}

def run_s5(driver, ctx):
    _run(driver, """
        MATCH (p:Profile {profile_id: $pid})-[w:WATCHED]->(c:Content)
        RETURN w.started_at, w.progress_percent, w.completed,
               c.content_id, c.title, c.type
        ORDER BY w.started_at DESC LIMIT 50
    """, {"pid": ctx["profile_id"]})

def teardown_s5(driver, ctx):
    return


def setup_s6(driver):
    rows = _run(driver, """
        MATCH (p:Person)-[:ACTED_IN]->(c:Content)
        WITH p, count(c) AS cnt ORDER BY cnt DESC LIMIT 1
        RETURN p.person_id AS pid
    """)
    if not rows:
        raise RuntimeError("Brak danych ACTED_IN")
    return {"person_id": rows[0]["pid"]}

def run_s6(driver, ctx):
    _run(driver, """
        MATCH (p:Person {person_id: $pid})-[r]->(c:Content)
        RETURN c.content_id, c.title, c.type, c.release_date, type(r) AS role
        ORDER BY c.release_date DESC
    """, {"pid": ctx["person_id"]})

def teardown_s6(driver, ctx):
    return


# ═════════════════════════════════════════════════════════════
# UPDATE — 6 scenariuszy
# ═════════════════════════════════════════════════════════════

def setup_u1(driver):
    rows = _run(driver, """
        MATCH (p:Profile)-[w:WATCHED]->(c:Content)
        RETURN elementId(w) AS rel_id, w.progress_percent AS old_progress LIMIT 1
    """)
    if not rows:
        raise RuntimeError("Brak relacji WATCHED")
    return {"rel_id": rows[0]["rel_id"], "old_progress": rows[0]["old_progress"]}

def run_u1(driver, ctx):
    new_progress = round(random.uniform(10, 99), 2)
    _run_write(driver, """
        MATCH ()-[w:WATCHED]->()
        WHERE elementId(w) = $rid
        SET w.progress_percent = $progress
    """, {"rid": ctx["rel_id"], "progress": new_progress})

def teardown_u1(driver, ctx):
    _run_write(driver, """
        MATCH ()-[w:WATCHED]->()
        WHERE elementId(w) = $rid
        SET w.progress_percent = $progress
    """, {"rid": ctx["rel_id"], "progress": ctx["old_progress"]})


def setup_u2(driver):
    rows = _run(driver, """
        MATCH ()-[r:RATED]->(c:Content)
        WITH c, count(r) AS cnt ORDER BY cnt DESC LIMIT 1
        RETURN c.content_id AS cid, c.avg_rating AS old_avg
    """)
    if not rows:
        raise RuntimeError("Brak relacji RATED")
    return {"content_id": rows[0]["cid"], "old_avg": rows[0]["old_avg"]}

def run_u2(driver, ctx):
    rows = _run(driver, """
        MATCH ()-[r:RATED]->(c:Content {content_id: $cid})
        RETURN avg(r.score) AS avg_score
    """, {"cid": ctx["content_id"]})
    new_avg = rows[0]["avg_score"] if rows else 0.0
    _run_write(driver, """
        MATCH (c:Content {content_id: $cid}) SET c.avg_rating = $avg
    """, {"cid": ctx["content_id"], "avg": round(new_avg or 0.0, 2)})

def teardown_u2(driver, ctx):
    return


def setup_u3(driver):
    rows = _run(driver, """
        MATCH (u:User)-[:HAS_SUBSCRIPTION]->(s:Subscription {status: 'active'})
        RETURN s.subscription_id AS sid, s.plan_name AS plan
        LIMIT 500
    """)
    if not rows:
        raise RuntimeError("Brak aktywnych subskrypcji")
    return {"originals": [(r["sid"], r["plan"]) for r in rows]}

def run_u3(driver, ctx):
    ids = [r[0] for r in ctx["originals"]]
    _run_write(driver, """
        UNWIND $ids AS sid
        MATCH (s:Subscription {subscription_id: sid})
        SET s.plan_name = 'premium'
    """, {"ids": ids})

def teardown_u3(driver, ctx):
    with driver.session() as session:
        for sid, plan in ctx["originals"]:
            session.run("""
                MATCH (s:Subscription {subscription_id: $sid}) SET s.plan_name = $plan
            """, {"sid": sid, "plan": plan})


def setup_u4(driver):
    rows = _run(driver, "MATCH (u:User) RETURN u.user_id AS uid, u.email AS email LIMIT 1")
    if not rows:
        raise RuntimeError("Brak userow")
    return {"user_id": rows[0]["uid"], "old_email": rows[0]["email"]}

def run_u4(driver, ctx):
    _run_write(driver, """
        MATCH (u:User {user_id: $uid})
        SET u.email = $email
    """, {"uid": ctx["user_id"], "email": _rand_email()})

def teardown_u4(driver, ctx):
    _run_write(driver, """
        MATCH (u:User {user_id: $uid}) SET u.email = $email
    """, {"uid": ctx["user_id"], "email": ctx["old_email"]})


def setup_u5(driver):
    rows = _run(driver, "MATCH (c:Content {is_active: true}) RETURN c.content_id AS cid LIMIT 1")
    if not rows:
        raise RuntimeError("Brak aktywnych tresci")
    return {"content_id": rows[0]["cid"]}

def run_u5(driver, ctx):
    _run_write(driver, """
        MATCH (c:Content {content_id: $cid}) SET c.is_active = false
    """, {"cid": ctx["content_id"]})

def teardown_u5(driver, ctx):
    _run_write(driver, """
        MATCH (c:Content {content_id: $cid}) SET c.is_active = true
    """, {"cid": ctx["content_id"]})


def setup_u6(driver):
    return {}

def run_u6(driver, ctx):
    _run_write(driver, """
        MATCH (c:Content {is_active: true})
        SET c.popularity_score = round(
            c.avg_rating * (toFloat(c.total_views) / 10000.0 + 1.0) * 100
        ) / 100.0
    """)

def teardown_u6(driver, ctx):
    return


# ═════════════════════════════════════════════════════════════
# DELETE — 6 scenariuszy
# ═════════════════════════════════════════════════════════════

def setup_d1(driver):
    content_id = random.randint(9_000_000, 9_999_999)
    _run_write(driver, """
        CREATE (c:Content {content_id: $cid, title: $title, type: 'series',
                           maturity_rating: 'ALL', is_active: true,
                           avg_rating: 0.0, total_views: 0, popularity_score: 0.0,
                           created_at: $now})
    """, {"cid": content_id, "title": f"Do usuniecia {_rand_str()}",
          "now": datetime.now().isoformat()})

    for sn in range(1, 3):
        sid = content_id * 10 + sn
        _run_write(driver, """
            MATCH (c:Content {content_id: $cid})
            CREATE (s:Season {season_id: $sid, season_number: $sn, title: $title})
            CREATE (c)-[:HAS_SEASON]->(s)
        """, {"cid": content_id, "sid": sid, "sn": sn, "title": f"Sezon {sn}"})

        for ep in range(1, 4):
            eid = sid * 10 + ep
            _run_write(driver, """
                MATCH (s:Season {season_id: $sid})
                CREATE (e:Episode {episode_id: $eid, episode_number: $en,
                                   title: $title, duration_minutes: 40})
                CREATE (s)-[:HAS_EPISODE]->(e)
            """, {"sid": sid, "eid": eid, "en": ep, "title": f"Ep {ep}"})

    # Dodaj kilka relacji WATCHED
    profile_ids = _get_random_profile_ids(driver, 3)
    for pid in profile_ids:
        _run_write(driver, """
            MATCH (p:Profile {profile_id: $pid})
            MATCH (c:Content {content_id: $cid})
            CREATE (p)-[:WATCHED {started_at: $now, progress_percent: 50.0, completed: false}]->(c)
        """, {"pid": pid, "cid": content_id, "now": datetime.now().isoformat()})

    return {"content_id": content_id}

def run_d1(driver, ctx):
    _run_write(driver, """
        MATCH (c:Content {content_id: $cid})
        OPTIONAL MATCH (c)-[:HAS_SEASON]->(s:Season)
        OPTIONAL MATCH (s)-[:HAS_EPISODE]->(e:Episode)
        DETACH DELETE c, s, e
    """, {"cid": ctx["content_id"]})

def teardown_d1(driver, ctx):
    return


def setup_d2(driver):
    user_id = random.randint(9_000_000, 9_999_999)
    profile_id = user_id * 10
    _run_write(driver, """
        CREATE (u:User {user_id: $uid, email: $email,
                        first_name: 'Test', last_name: 'D2',
                        country_code: 'PL', status: 'active',
                        created_at: $now})
        CREATE (p:Profile {profile_id: $pid, name: 'Profil D2',
                           is_kids: false, maturity_rating: 'ALL', language: 'pl'})
        CREATE (u)-[:HAS_PROFILE]->(p)
    """, {"uid": user_id, "pid": profile_id, "email": _rand_email(),
          "now": datetime.now().isoformat()})

    # Dodaj kilka relacji WATCHED dla profilu
    content_ids = _get_random_content_ids(driver, 3)
    for cid in content_ids:
        _run_write(driver, """
            MATCH (p:Profile {profile_id: $pid})
            MATCH (c:Content {content_id: $cid})
            CREATE (p)-[:WATCHED {started_at: $now, progress_percent: 30.0, completed: false}]->(c)
        """, {"pid": profile_id, "cid": cid, "now": datetime.now().isoformat()})

    return {"user_id": user_id, "profile_id": profile_id}

def run_d2(driver, ctx):
    _run_write(driver, """
        MATCH (p:Profile {profile_id: $pid})
        DETACH DELETE p
    """, {"pid": ctx["profile_id"]})

def teardown_d2(driver, ctx):
    _run_write(driver, """
        MATCH (u:User {user_id: $uid}) DETACH DELETE u
    """, {"uid": ctx["user_id"]})


def setup_d3(driver):
    # Wstaw 100 starych relacji WATCHED
    profile_ids = _get_random_profile_ids(driver, 5)
    content_ids = _get_random_content_ids(driver, 10)
    old_date = (datetime.now() - timedelta(days=800)).isoformat()

    rows = [
        {
            "pid": random.choice(profile_ids),
            "cid": random.choice(content_ids),
            "started_at": old_date,
        }
        for _ in range(100)
    ]
    with driver.session() as session:
        session.run("""
            UNWIND $rows AS r
            MATCH (p:Profile {profile_id: r.pid})
            MATCH (c:Content {content_id: r.cid})
            CREATE (p)-[:WATCHED {started_at: r.started_at,
                                   progress_percent: 0.0, completed: false,
                                   _test_marker: true}]->(c)
        """, {"rows": rows})
    return {}

def run_d3(driver, ctx):
    cutoff = (datetime.now() - timedelta(days=365)).isoformat()
    _run_write(driver, """
        MATCH ()-[w:WATCHED]->()
        WHERE w.started_at < $cutoff AND w._test_marker = true
        DELETE w
    """, {"cutoff": cutoff})

def teardown_d3(driver, ctx):
    return


def setup_d4(driver):
    profile_ids = _get_random_profile_ids(driver, 1)
    content_ids = _get_random_content_ids(driver, 1)
    if not profile_ids or not content_ids:
        raise RuntimeError("Brak danych")
    profile_id = profile_ids[0]
    content_id = content_ids[0]

    _run_write(driver, """
        MATCH (p:Profile {profile_id: $pid})
        MATCH (c:Content {content_id: $cid})
        MERGE (p)-[:ADDED_TO_LIST {added_at: $now}]->(c)
    """, {"pid": profile_id, "cid": content_id, "now": datetime.now().isoformat()})

    return {"profile_id": profile_id, "content_id": content_id}

def run_d4(driver, ctx):
    _run_write(driver, """
        MATCH (p:Profile {profile_id: $pid})-[r:ADDED_TO_LIST]->(c:Content {content_id: $cid})
        DELETE r
    """, {"pid": ctx["profile_id"], "cid": ctx["content_id"]})

def teardown_d4(driver, ctx):
    return


def setup_d5(driver):
    user_id = random.randint(9_000_000, 9_999_999)
    sub_id = user_id * 10
    _run_write(driver, """
        CREATE (u:User {user_id: $uid, email: $email,
                        first_name: 'Test', last_name: 'D5',
                        country_code: 'PL', status: 'active',
                        created_at: $now})
        CREATE (s:Subscription {subscription_id: $sid, plan_name: 'basic',
                                price_monthly: 29.99, status: 'active'})
        CREATE (u)-[:HAS_SUBSCRIPTION]->(s)
    """, {"uid": user_id, "sid": sub_id, "email": _rand_email(),
          "now": datetime.now().isoformat()})

    # 5 platnosci
    with driver.session() as session:
        pays = [{"pid": sub_id * 10 + i, "sid": sub_id} for i in range(5)]
        session.run("""
            UNWIND $pays AS p
            MATCH (s:Subscription {subscription_id: p.sid})
            CREATE (pay:Payment {payment_id: p.pid, amount: 29.99,
                                  currency: 'PLN', status: 'completed'})
            CREATE (s)-[:HAS_PAYMENT]->(pay)
        """, {"pays": pays})

    return {"user_id": user_id, "sub_id": sub_id}

def run_d5(driver, ctx):
    _run_write(driver, """
        MATCH (s:Subscription {subscription_id: $sid})
        OPTIONAL MATCH (s)-[:HAS_PAYMENT]->(pay:Payment)
        DETACH DELETE s, pay
    """, {"sid": ctx["sub_id"]})

def teardown_d5(driver, ctx):
    _run_write(driver, """
        MATCH (u:User {user_id: $uid}) DETACH DELETE u
    """, {"uid": ctx["user_id"]})


def setup_d6(driver):
    rows = _run(driver, "MATCH (u:User) RETURN max(u.user_id) AS m")
    max_id = rows[0]["m"] or 0

    nationalities = ["PL", "US", "GB"]
    users = [
        {
            "uid": max_id + i + 1,
            "email": _rand_email(),
            "status": "deleted",
        }
        for i in range(50)
    ]
    with driver.session() as session:
        session.run("""
            UNWIND $users AS u
            CREATE (:User {user_id: u.uid, email: u.email,
                           first_name: 'Del', last_name: 'User',
                           country_code: 'PL', status: u.status,
                           created_at: $now})
        """, {"users": users, "now": datetime.now().isoformat()})

    return {"max_id": max_id}

def run_d6(driver, ctx):
    _run_write(driver, """
        MATCH (u:User)
        WHERE u.status = 'deleted' AND u.user_id > $max_id
        DETACH DELETE u
    """, {"max_id": ctx["max_id"]})

def teardown_d6(driver, ctx):
    return


# ═════════════════════════════════════════════════════════════
# Lista wszystkich scenariuszy (uzywana przez runner.py)
# ═════════════════════════════════════════════════════════════

SCENARIOS = [
    {"id": "I1", "name": "Rejestracja uzytkownika (User+Profile+Subscription+relacje)",
     "setup": setup_i1, "run": run_i1, "teardown": teardown_i1},
    {"id": "I2", "name": "Batch insert relacji WATCHED (1000)",
     "setup": setup_i2, "run": run_i2, "teardown": teardown_i2},
    {"id": "I3", "name": "Dodanie serialu z drzewem (Content+Season+Episode+ACTED_IN)",
     "setup": setup_i3, "run": run_i3, "teardown": teardown_i3},
    {"id": "I4", "name": "Batch insert wezlow Payment (1000)",
     "setup": setup_i4, "run": run_i4, "teardown": teardown_i4},
    {"id": "I5", "name": "Dodanie relacji RATED z przeliczeniem avg_rating",
     "setup": setup_i5, "run": run_i5, "teardown": teardown_i5},
    {"id": "I6", "name": "Import Person z relacjami ACTED_IN (100 osob)",
     "setup": setup_i6, "run": run_i6, "teardown": teardown_i6},

    {"id": "S1", "name": "Strona glowna (filtrowanie + sortowanie)",
     "setup": setup_s1, "run": run_s1, "teardown": teardown_s1},
    {"id": "S2", "name": "Rekomendacje collaborative filtering (graph traversal)",
     "setup": setup_s2, "run": run_s2, "teardown": teardown_s2},
    {"id": "S3", "name": "TOP 100 tresci wg ogladalnosci (ostatni miesiac)",
     "setup": setup_s3, "run": run_s3, "teardown": teardown_s3},
    {"id": "S4", "name": "Wyszukiwanie po tytule (CONTAINS)",
     "setup": setup_s4, "run": run_s4, "teardown": teardown_s4},
    {"id": "S5", "name": "Historia ogladania profilu (50 ostatnich relacji WATCHED)",
     "setup": setup_s5, "run": run_s5, "teardown": teardown_s5},
    {"id": "S6", "name": "Filmografia osoby (ACTED_IN + DIRECTED + WROTE)",
     "setup": setup_s6, "run": run_s6, "teardown": teardown_s6},

    {"id": "U1", "name": "Aktualizacja postepu ogladania (rel WATCHED)",
     "setup": setup_u1, "run": run_u1, "teardown": teardown_u1},
    {"id": "U2", "name": "Przeliczenie avg_rating (avg relacji RATED)",
     "setup": setup_u2, "run": run_u2, "teardown": teardown_u2},
    {"id": "U3", "name": "Masowa zmiana planu subskrypcji (500 wezlow)",
     "setup": setup_u3, "run": run_u3, "teardown": teardown_u3},
    {"id": "U4", "name": "Aktualizacja email uzytkownika",
     "setup": setup_u4, "run": run_u4, "teardown": teardown_u4},
    {"id": "U5", "name": "Oznaczenie tresci jako nieaktywnej",
     "setup": setup_u5, "run": run_u5, "teardown": teardown_u5},
    {"id": "U6", "name": "Masowa aktualizacja popularity_score (formula)",
     "setup": setup_u6, "run": run_u6, "teardown": teardown_u6},

    {"id": "D1", "name": "Usuniecie tresci z kaskada (DETACH DELETE + sezon/odcinki)",
     "setup": setup_d1, "run": run_d1, "teardown": teardown_d1},
    {"id": "D2", "name": "Usuniecie profilu z historia (DETACH DELETE Profile)",
     "setup": setup_d2, "run": run_d2, "teardown": teardown_d2},
    {"id": "D3", "name": "Czyszczenie starej historii (DELETE relacji WATCHED)",
     "setup": setup_d3, "run": run_d3, "teardown": teardown_d3},
    {"id": "D4", "name": "Usuniecie relacji ADDED_TO_LIST",
     "setup": setup_d4, "run": run_d4, "teardown": teardown_d4},
    {"id": "D5", "name": "Usuniecie subskrypcji z platnosciami (DETACH DELETE)",
     "setup": setup_d5, "run": run_d5, "teardown": teardown_d5},
    {"id": "D6", "name": "Masowe usuniecie userow status=deleted",
     "setup": setup_d6, "run": run_d6, "teardown": teardown_d6},
]

# Automatycznie uzupelnij pole "category" na podstawie prefiksu id
_CATEGORY_MAP = {"I": "INSERT", "S": "SELECT", "U": "UPDATE", "D": "DELETE"}
for _s in SCENARIOS:
    _s["category"] = _CATEGORY_MAP[_s["id"][0]]
