"""
Scenariusze benchmarkowe dla MySQL (pymysql).

Identyczne 24 scenariusze co dla PostgreSQL, dostosowane do MySQL:
- brak RETURNING — uzywamy LAST_INSERT_ID()
- ORDER BY RAND() zamiast RANDOM()
- INSERT IGNORE zamiast ON CONFLICT DO NOTHING
- Interval: INTERVAL 30 DAY (nie '30 days')
"""

import random
import uuid
from datetime import date, datetime, timedelta


# ─────────────────────────────────────────────────────────────
# Pomocnicze funkcje
# ─────────────────────────────────────────────────────────────

def _rand_email():
    return f"test_{uuid.uuid4().hex[:12]}@benchmark.test"

def _rand_str(length=8):
    return uuid.uuid4().hex[:length]

def _last_insert_id(conn):
    """Pobiera ID ostatnio wstawionego wiersza."""
    with conn.cursor() as cur:
        cur.execute("SELECT LAST_INSERT_ID()")
        return cur.fetchone()[0]


# ═════════════════════════════════════════════════════════════
# INSERT — 6 scenariuszy
# ═════════════════════════════════════════════════════════════

# ─── I1: Rejestracja uzytkownika ─────────────────────────────

def setup_i1(conn):
    return {}

def run_i1(conn, ctx):
    email = _rand_email()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (email, password_hash, first_name, last_name,
                               date_of_birth, country_code, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (email, "$2b$12$testhash", "Jan", "Testowy", "1990-01-01", "PL", "active"))
        user_id = _last_insert_id(conn)

        cur.execute("""
            INSERT INTO profiles (user_id, name, maturity_rating, language)
            VALUES (%s, %s, %s, %s)
        """, (user_id, "Glowny", "ALL", "pl"))

        cur.execute("""
            INSERT INTO subscriptions
                (user_id, plan_name, price_monthly, max_streams,
                 max_resolution, status, start_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (user_id, "basic", 29.99, 1, "HD", "active", date.today()))

    conn.commit()
    ctx["user_id"] = user_id

def teardown_i1(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM users WHERE user_id = %s", (ctx["user_id"],))
    conn.commit()


# ─── I2: Masowy import watch_history (1000 rekordow) ─────────

def setup_i2(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(watch_id), 0) FROM watch_history")
        max_id = cur.fetchone()[0]
        cur.execute("SELECT profile_id FROM profiles LIMIT 200")
        profile_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT content_id FROM content LIMIT 200")
        content_ids = [r[0] for r in cur.fetchall()]
    return {"max_id": max_id, "profile_ids": profile_ids, "content_ids": content_ids}

def run_i2(conn, ctx):
    rows = [
        (random.choice(ctx["profile_ids"]), random.choice(ctx["content_ids"]),
         round(random.uniform(0, 100), 2), False)
        for _ in range(1000)
    ]
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO watch_history (profile_id, content_id, progress_percent, completed)
            VALUES (%s, %s, %s, %s)
        """, rows)
    conn.commit()

def teardown_i2(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM watch_history WHERE watch_id > %s", (ctx["max_id"],))
    conn.commit()


# ─── I3: Dodanie serialu z pelnym drzewem ────────────────────

def setup_i3(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT person_id FROM people LIMIT 5")
        person_ids = [r[0] for r in cur.fetchall()]
    return {"person_ids": person_ids}

def run_i3(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO content (title, type, maturity_rating, is_active)
            VALUES (%s, %s, %s, %s)
        """, (f"Serial testowy {_rand_str()}", "series", "ALL", True))
        content_id = _last_insert_id(conn)

        for season_no in range(1, 3):
            cur.execute("""
                INSERT INTO seasons (content_id, season_number, title)
                VALUES (%s, %s, %s)
            """, (content_id, season_no, f"Sezon {season_no}"))
            season_id = _last_insert_id(conn)

            for ep_no in range(1, 4):
                cur.execute("""
                    INSERT INTO episodes (season_id, episode_number, title, duration_minutes)
                    VALUES (%s, %s, %s, %s)
                """, (season_id, ep_no, f"Odcinek {ep_no}", 45))

        for person_id in ctx["person_ids"][:3]:
            cur.execute("""
                INSERT IGNORE INTO content_people (content_id, person_id, role, billing_order)
                VALUES (%s, %s, %s, %s)
            """, (content_id, person_id, "actor", 1))

    conn.commit()
    ctx["content_id"] = content_id

def teardown_i3(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM content WHERE content_id = %s", (ctx["content_id"],))
    conn.commit()


# ─── I4: Batch insert platnosci (1000 rekordow) ──────────────

def setup_i4(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(payment_id), 0) FROM payments")
        max_id = cur.fetchone()[0]
        cur.execute("SELECT subscription_id FROM subscriptions LIMIT 200")
        sub_ids = [r[0] for r in cur.fetchall()]
    return {"max_id": max_id, "sub_ids": sub_ids}

def run_i4(conn, ctx):
    methods = ["credit_card", "blik", "transfer", "paypal"]
    rows = [
        (random.choice(ctx["sub_ids"]), round(random.uniform(19, 79), 2),
         "PLN", random.choice(methods),
         f"TXN-{uuid.uuid4().hex[:16].upper()}", "completed")
        for _ in range(1000)
    ]
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO payments (subscription_id, amount, currency,
                                  payment_method, transaction_id, status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, rows)
    conn.commit()

def teardown_i4(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM payments WHERE payment_id > %s", (ctx["max_id"],))
    conn.commit()


# ─── I5: Dodanie oceny z przeliczeniem avg_rating ────────────

def setup_i5(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.content_id, p.profile_id, c.avg_rating
            FROM content c
            JOIN profiles p
            WHERE NOT EXISTS (
                SELECT 1 FROM ratings r
                WHERE r.content_id = c.content_id AND r.profile_id = p.profile_id
            )
            LIMIT 1
        """)
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("Brak wolnej pary (content_id, profile_id) bez oceny")
    return {"content_id": row[0], "profile_id": row[1], "old_avg": row[2]}

def run_i5(conn, ctx):
    score = random.randint(1, 10)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ratings (profile_id, content_id, score)
            VALUES (%s, %s, %s)
        """, (ctx["profile_id"], ctx["content_id"], score))
        # MySQL nie pozwala na subquery tej samej tabeli — derived table jako obejscie
        cur.execute("""
            UPDATE content
            SET avg_rating = (
                SELECT avg_score FROM (
                    SELECT AVG(score) AS avg_score FROM ratings WHERE content_id = %s
                ) AS tmp
            )
            WHERE content_id = %s
        """, (ctx["content_id"], ctx["content_id"]))
    conn.commit()

def teardown_i5(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM ratings WHERE profile_id = %s AND content_id = %s",
                    (ctx["profile_id"], ctx["content_id"]))
        cur.execute("UPDATE content SET avg_rating = %s WHERE content_id = %s",
                    (ctx["old_avg"], ctx["content_id"]))
    conn.commit()


# ─── I6: Import osob z powiazaniami (100 osob) ───────────────

def setup_i6(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(person_id), 0) FROM people")
        max_id = cur.fetchone()[0]
        cur.execute("SELECT content_id FROM content ORDER BY RAND() LIMIT 100")
        content_ids = [r[0] for r in cur.fetchall()]
    return {"max_id": max_id, "content_ids": content_ids}

def run_i6(conn, ctx):
    nationalities = ["PL", "US", "GB", "DE", "FR"]
    people_rows = [
        (f"Imie{i}", f"Nazwisko{i}",
         date(random.randint(1960, 2000), random.randint(1, 12), 1),
         random.choice(nationalities))
        for i in range(100)
    ]
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO people (first_name, last_name, birth_date, nationality)
            VALUES (%s, %s, %s, %s)
        """, people_rows)

        cur.execute("SELECT person_id FROM people WHERE person_id > %s", (ctx["max_id"],))
        new_person_ids = [r[0] for r in cur.fetchall()]

        cp_rows = [
            (random.choice(ctx["content_ids"]), pid, "actor", idx + 1)
            for idx, pid in enumerate(new_person_ids)
        ]
        cur.executemany("""
            INSERT IGNORE INTO content_people (content_id, person_id, role, billing_order)
            VALUES (%s, %s, %s, %s)
        """, cp_rows)
    conn.commit()

def teardown_i6(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM people WHERE person_id > %s", (ctx["max_id"],))
    conn.commit()


# ═════════════════════════════════════════════════════════════
# SELECT — 6 scenariuszy
# ═════════════════════════════════════════════════════════════

def setup_s1(conn):
    return {}

def run_s1(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT content_id, title, type, genres, avg_rating,
                   popularity_score, maturity_rating
            FROM content
            WHERE is_active = TRUE
            ORDER BY popularity_score DESC
            LIMIT 20
        """)
        cur.fetchall()

def teardown_s1(conn, ctx):
    return


def setup_s2(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT profile_id FROM watch_history
            GROUP BY profile_id ORDER BY COUNT(*) DESC LIMIT 1
        """)
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("Brak danych w watch_history")
    return {"profile_id": row[0]}

def run_s2(conn, ctx):
    pid = ctx["profile_id"]
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.content_id, c.title, c.avg_rating, COUNT(*) AS score
            FROM watch_history wh1
            JOIN watch_history wh2
              ON wh1.content_id = wh2.content_id
             AND wh1.profile_id  != wh2.profile_id
            JOIN content c ON c.content_id = wh2.content_id
            WHERE wh1.profile_id = %s
              AND wh2.content_id NOT IN (
                    SELECT content_id FROM watch_history WHERE profile_id = %s
              )
              AND c.is_active = TRUE
            GROUP BY c.content_id, c.title, c.avg_rating
            ORDER BY score DESC
            LIMIT 10
        """, (pid, pid))
        cur.fetchall()

def teardown_s2(conn, ctx):
    return


def setup_s3(conn):
    return {}

def run_s3(conn, ctx):
    with conn.cursor() as cur:
        # MySQL: INTERVAL 30 DAY (bez apostrofu)
        cur.execute("""
            SELECT wh.content_id, c.title, COUNT(*) AS views
            FROM watch_history wh
            JOIN content c ON c.content_id = wh.content_id
            WHERE wh.started_at >= NOW() - INTERVAL 30 DAY
            GROUP BY wh.content_id, c.title
            ORDER BY views DESC
            LIMIT 100
        """)
        cur.fetchall()

def teardown_s3(conn, ctx):
    return


def setup_s4(conn):
    return {}

def run_s4(conn, ctx):
    keyword = random.choice(["the", "man", "love", "war", "world"])
    with conn.cursor() as cur:
        cur.execute("""
            SELECT content_id, title, type, avg_rating
            FROM content
            WHERE title LIKE %s AND is_active = TRUE
            LIMIT 20
        """, (f"%{keyword}%",))
        cur.fetchall()

def teardown_s4(conn, ctx):
    return


def setup_s5(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT profile_id FROM watch_history
            GROUP BY profile_id ORDER BY COUNT(*) DESC LIMIT 1
        """)
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("Brak danych w watch_history")
    return {"profile_id": row[0]}

def run_s5(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT wh.watch_id, wh.started_at, wh.progress_percent,
                   wh.completed, c.title, c.type
            FROM watch_history wh
            JOIN content c ON c.content_id = wh.content_id
            WHERE wh.profile_id = %s
            ORDER BY wh.started_at DESC
            LIMIT 50
        """, (ctx["profile_id"],))
        cur.fetchall()

def teardown_s5(conn, ctx):
    return


def setup_s6(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT person_id FROM content_people
            GROUP BY person_id ORDER BY COUNT(*) DESC LIMIT 1
        """)
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("Brak danych w content_people")
    return {"person_id": row[0]}

def run_s6(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.content_id, c.title, c.type, c.release_date,
                   cp.role, cp.character_name
            FROM content_people cp
            JOIN content c ON c.content_id = cp.content_id
            WHERE cp.person_id = %s
            ORDER BY c.release_date DESC
        """, (ctx["person_id"],))
        cur.fetchall()

def teardown_s6(conn, ctx):
    return


# ═════════════════════════════════════════════════════════════
# UPDATE — 6 scenariuszy
# ═════════════════════════════════════════════════════════════

def setup_u1(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT watch_id, progress_percent FROM watch_history ORDER BY RAND() LIMIT 1")
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("Brak danych w watch_history")
    return {"watch_id": row[0], "old_progress": float(row[1])}

def run_u1(conn, ctx):
    new_progress = round(random.uniform(10, 99), 2)
    with conn.cursor() as cur:
        cur.execute("UPDATE watch_history SET progress_percent = %s WHERE watch_id = %s",
                    (new_progress, ctx["watch_id"]))
    conn.commit()

def teardown_u1(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("UPDATE watch_history SET progress_percent = %s WHERE watch_id = %s",
                    (ctx["old_progress"], ctx["watch_id"]))
    conn.commit()


def setup_u2(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT content_id FROM ratings
            GROUP BY content_id ORDER BY COUNT(*) DESC LIMIT 1
        """)
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("Brak danych w ratings")
    return {"content_id": row[0]}

def run_u2(conn, ctx):
    # MySQL nie pozwala subquery na tej samej tabeli — obejscie: derived table
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE content
            SET avg_rating = (
                SELECT avg_score FROM (
                    SELECT AVG(score) AS avg_score FROM ratings WHERE content_id = %s
                ) AS tmp
            )
            WHERE content_id = %s
        """, (ctx["content_id"], ctx["content_id"]))
    conn.commit()

def teardown_u2(conn, ctx):
    return


def setup_u3(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT subscription_id, plan_name FROM subscriptions
            WHERE status = 'active' LIMIT 500
        """)
        rows = cur.fetchall()
    if not rows:
        raise RuntimeError("Brak aktywnych subskrypcji")
    return {"originals": [(r[0], r[1]) for r in rows]}

def run_u3(conn, ctx):
    ids = [r[0] for r in ctx["originals"]]
    placeholders = ", ".join(["%s"] * len(ids))
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE subscriptions SET plan_name = 'premium' WHERE subscription_id IN ({placeholders})",
            ids
        )
    conn.commit()

def teardown_u3(conn, ctx):
    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE subscriptions SET plan_name = %s WHERE subscription_id = %s",
            [(plan, sid) for sid, plan in ctx["originals"]],
        )
    conn.commit()


def setup_u4(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT user_id, email, phone FROM users ORDER BY RAND() LIMIT 1")
        row = cur.fetchone()
    return {"user_id": row[0], "old_email": row[1], "old_phone": row[2]}

def run_u4(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE users SET email = %s, phone = %s, updated_at = NOW()
            WHERE user_id = %s
        """, (_rand_email(), "+48 100 200 300", ctx["user_id"]))
    conn.commit()

def teardown_u4(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("UPDATE users SET email = %s, phone = %s WHERE user_id = %s",
                    (ctx["old_email"], ctx["old_phone"], ctx["user_id"]))
    conn.commit()


def setup_u5(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT content_id FROM content WHERE is_active = TRUE LIMIT 1")
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("Brak aktywnych tresci")
    return {"content_id": row[0]}

def run_u5(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("UPDATE content SET is_active = FALSE WHERE content_id = %s",
                    (ctx["content_id"],))
    conn.commit()

def teardown_u5(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("UPDATE content SET is_active = TRUE WHERE content_id = %s",
                    (ctx["content_id"],))
    conn.commit()


def setup_u6(conn):
    return {}

def run_u6(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE content
            SET popularity_score = avg_rating * (total_views / 10000.0 + 1)
            WHERE is_active = TRUE
        """)
    conn.commit()

def teardown_u6(conn, ctx):
    return


# ═════════════════════════════════════════════════════════════
# DELETE — 6 scenariuszy
# ═════════════════════════════════════════════════════════════

def setup_d1(conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO content (title, type, maturity_rating, is_active)
            VALUES (%s, 'series', 'ALL', TRUE)
        """, (f"Do usuniecia {_rand_str()}",))
        content_id = _last_insert_id(conn)

        season_ids = []
        for sn in range(1, 3):
            cur.execute("""
                INSERT INTO seasons (content_id, season_number, title)
                VALUES (%s, %s, %s)
            """, (content_id, sn, f"Sezon {sn}"))
            season_ids.append(_last_insert_id(conn))

        for sid in season_ids:
            for en in range(1, 4):
                cur.execute("""
                    INSERT INTO episodes (season_id, episode_number, title, duration_minutes)
                    VALUES (%s, %s, %s, 40)
                """, (sid, en, f"Ep {en}"))

        cur.execute("SELECT profile_id FROM profiles LIMIT 5")
        profile_ids = [r[0] for r in cur.fetchall()]
        for pid in profile_ids:
            cur.execute("""
                INSERT INTO watch_history (profile_id, content_id, progress_percent, completed)
                VALUES (%s, %s, 50.0, FALSE)
            """, (pid, content_id))

    conn.commit()
    return {"content_id": content_id}

def run_d1(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM content WHERE content_id = %s", (ctx["content_id"],))
    conn.commit()

def teardown_d1(conn, ctx):
    return


def setup_d2(conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (email, password_hash, first_name, last_name,
                               date_of_birth, country_code)
            VALUES (%s, 'hash', 'Test', 'D2', '1990-01-01', 'PL')
        """, (_rand_email(),))
        user_id = _last_insert_id(conn)

        cur.execute("""
            INSERT INTO profiles (user_id, name, maturity_rating, language)
            VALUES (%s, 'Profil D2', 'ALL', 'pl')
        """, (user_id,))
        profile_id = _last_insert_id(conn)

        cur.execute("SELECT content_id FROM content LIMIT 5")
        content_ids = [r[0] for r in cur.fetchall()]
        for cid in content_ids:
            cur.execute("""
                INSERT INTO watch_history (profile_id, content_id, progress_percent, completed)
                VALUES (%s, %s, 30.0, FALSE)
            """, (profile_id, cid))

    conn.commit()
    return {"profile_id": profile_id, "user_id": user_id}

def run_d2(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM profiles WHERE profile_id = %s", (ctx["profile_id"],))
    conn.commit()

def teardown_d2(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM users WHERE user_id = %s", (ctx["user_id"],))
    conn.commit()


def setup_d3(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(watch_id), 0) FROM watch_history")
        max_id = cur.fetchone()[0]

        cur.execute("SELECT profile_id FROM profiles LIMIT 10")
        profile_ids = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT content_id FROM content LIMIT 20")
        content_ids = [r[0] for r in cur.fetchall()]

        old_date = datetime.now() - timedelta(days=800)
        rows = [
            (random.choice(profile_ids), random.choice(content_ids), old_date, 0.0, False)
            for _ in range(100)
        ]
        cur.executemany("""
            INSERT INTO watch_history
                (profile_id, content_id, started_at, progress_percent, completed)
            VALUES (%s, %s, %s, %s, %s)
        """, rows)
    conn.commit()
    return {"max_id": max_id}

def run_d3(conn, ctx):
    cutoff = datetime.now() - timedelta(days=365)
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM watch_history
            WHERE started_at < %s AND watch_id > %s
        """, (cutoff, ctx["max_id"]))
    conn.commit()

def teardown_d3(conn, ctx):
    return


def setup_d4(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT profile_id FROM profiles ORDER BY RAND() LIMIT 1")
        profile_id = cur.fetchone()[0]

        cur.execute("""
            SELECT content_id FROM content
            WHERE content_id NOT IN (
                SELECT content_id FROM my_list WHERE profile_id = %s
            )
            LIMIT 1
        """, (profile_id,))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Brak dostepnego content_id do my_list")
        content_id = row[0]

        cur.execute("""
            INSERT INTO my_list (profile_id, content_id, sort_order)
            VALUES (%s, %s, 0)
        """, (profile_id, content_id))
    conn.commit()
    return {"profile_id": profile_id, "content_id": content_id}

def run_d4(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM my_list WHERE profile_id = %s AND content_id = %s",
                    (ctx["profile_id"], ctx["content_id"]))
    conn.commit()

def teardown_d4(conn, ctx):
    return


def setup_d5(conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (email, password_hash, first_name, last_name,
                               date_of_birth, country_code)
            VALUES (%s, 'hash', 'Test', 'D5', '1990-01-01', 'PL')
        """, (_rand_email(),))
        user_id = _last_insert_id(conn)

        cur.execute("""
            INSERT INTO subscriptions
                (user_id, plan_name, price_monthly, max_streams,
                 max_resolution, status, start_date)
            VALUES (%s, 'basic', 29.99, 1, 'HD', 'active', CURDATE())
        """, (user_id,))
        sub_id = _last_insert_id(conn)

        for _ in range(5):
            cur.execute("""
                INSERT INTO payments (subscription_id, amount, currency,
                                      payment_method, transaction_id, status)
                VALUES (%s, 29.99, 'PLN', 'blik', %s, 'completed')
            """, (sub_id, f"TXN-{uuid.uuid4().hex[:12].upper()}"))

    conn.commit()
    return {"subscription_id": sub_id, "user_id": user_id}

def run_d5(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM subscriptions WHERE subscription_id = %s",
                    (ctx["subscription_id"],))
    conn.commit()

def teardown_d5(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM users WHERE user_id = %s", (ctx["user_id"],))
    conn.commit()


def setup_d6(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(user_id), 0) FROM users")
        max_id = cur.fetchone()[0]

        rows = [
            (_rand_email(), "hash", "Del", f"User{i}", "1980-01-01", "PL", "deleted")
            for i in range(50)
        ]
        cur.executemany("""
            INSERT INTO users (email, password_hash, first_name, last_name,
                               date_of_birth, country_code, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, rows)
    conn.commit()
    return {"max_id": max_id}

def run_d6(conn, ctx):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM users WHERE status = 'deleted' AND user_id > %s",
                    (ctx["max_id"],))
    conn.commit()

def teardown_d6(conn, ctx):
    return


# ═════════════════════════════════════════════════════════════
# Lista wszystkich scenariuszy (uzywana przez runner.py)
# ═════════════════════════════════════════════════════════════

SCENARIOS = [
    {"id": "I1", "name": "Rejestracja uzytkownika (multi-table)",
     "setup": setup_i1, "run": run_i1, "teardown": teardown_i1},
    {"id": "I2", "name": "Batch insert watch_history (1000 rekordow)",
     "setup": setup_i2, "run": run_i2, "teardown": teardown_i2},
    {"id": "I3", "name": "Dodanie serialu z drzewem (content+seasons+episodes+cast)",
     "setup": setup_i3, "run": run_i3, "teardown": teardown_i3},
    {"id": "I4", "name": "Batch insert platnosci (1000 rekordow)",
     "setup": setup_i4, "run": run_i4, "teardown": teardown_i4},
    {"id": "I5", "name": "Dodanie oceny z przeliczeniem avg_rating",
     "setup": setup_i5, "run": run_i5, "teardown": teardown_i5},
    {"id": "I6", "name": "Import osob z powiazaniami (100 osob)",
     "setup": setup_i6, "run": run_i6, "teardown": teardown_i6},

    {"id": "S1", "name": "Strona glowna (filtrowanie + sortowanie)",
     "setup": setup_s1, "run": run_s1, "teardown": teardown_s1},
    {"id": "S2", "name": "Rekomendacje collaborative filtering",
     "setup": setup_s2, "run": run_s2, "teardown": teardown_s2},
    {"id": "S3", "name": "TOP 100 tresci wg ogladalnosci (ostatni miesiac)",
     "setup": setup_s3, "run": run_s3, "teardown": teardown_s3},
    {"id": "S4", "name": "Wyszukiwanie pelnotekstowe po tytule (LIKE)",
     "setup": setup_s4, "run": run_s4, "teardown": teardown_s4},
    {"id": "S5", "name": "Historia ogladania profilu (50 ostatnich z JOIN)",
     "setup": setup_s5, "run": run_s5, "teardown": teardown_s5},
    {"id": "S6", "name": "Filmografia osoby (JOIN people->content)",
     "setup": setup_s6, "run": run_s6, "teardown": teardown_s6},

    {"id": "U1", "name": "Aktualizacja postepu ogladania",
     "setup": setup_u1, "run": run_u1, "teardown": teardown_u1},
    {"id": "U2", "name": "Przeliczenie avg_rating (podzapytanie AVG)",
     "setup": setup_u2, "run": run_u2, "teardown": teardown_u2},
    {"id": "U3", "name": "Masowa zmiana planu subskrypcji (500 rekordow)",
     "setup": setup_u3, "run": run_u3, "teardown": teardown_u3},
    {"id": "U4", "name": "Aktualizacja danych uzytkownika (email+phone)",
     "setup": setup_u4, "run": run_u4, "teardown": teardown_u4},
    {"id": "U5", "name": "Oznaczenie tresci jako nieaktywnej",
     "setup": setup_u5, "run": run_u5, "teardown": teardown_u5},
    {"id": "U6", "name": "Masowa aktualizacja popularity_score (formula)",
     "setup": setup_u6, "run": run_u6, "teardown": teardown_u6},

    {"id": "D1", "name": "Usuniecie tresci z kaskada (CASCADE)",
     "setup": setup_d1, "run": run_d1, "teardown": teardown_d1},
    {"id": "D2", "name": "Usuniecie profilu z historia (CASCADE)",
     "setup": setup_d2, "run": run_d2, "teardown": teardown_d2},
    {"id": "D3", "name": "Czyszczenie starej historii ogladania",
     "setup": setup_d3, "run": run_d3, "teardown": teardown_d3},
    {"id": "D4", "name": "Usuniecie pozycji z my_list",
     "setup": setup_d4, "run": run_d4, "teardown": teardown_d4},
    {"id": "D5", "name": "Usuniecie subskrypcji z platnosciami (CASCADE)",
     "setup": setup_d5, "run": run_d5, "teardown": teardown_d5},
    {"id": "D6", "name": "Masowe usuniecie uzytkownikow status=deleted",
     "setup": setup_d6, "run": run_d6, "teardown": teardown_d6},
]

# Automatycznie uzupelnij pole "category" na podstawie prefiksu id
_CATEGORY_MAP = {"I": "INSERT", "S": "SELECT", "U": "UPDATE", "D": "DELETE"}
for _s in SCENARIOS:
    _s["category"] = _CATEGORY_MAP[_s["id"][0]]
