#!/usr/bin/env python3
"""Geopolitics watcher.

Scans trusted feeds, keeps only military/diplomatic items in known hotspots,
groups reports of the same event, and marks an event "verified" only when
enough independent sources report it. Writes web/data.json and serves web/.

Usage:  python watcher.py          # scan loop + web server
        python watcher.py --once   # single scan, no server (cron/testing)
"""
import csv
import hmac
import html
import io
import json
import math
import operator
import re
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

import zones as zone_store
from pathlib import Path

ROOT = Path(__file__).parent
WEB = ROOT / "web"
DB = ROOT / "data" / "watcher.db"
CFG = json.loads((ROOT / "config.json").read_text())
# Machine-specific settings (ntfy topic, thread count, ...) live in config.local.json, which git ignores,
# so pulling updates never overwrites them. Nested settings are merged, not replaced.
def merge(base, override):
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merge(base[key], value)
        else:
            base[key] = value


LOCAL_CFG = ROOT / "config.local.json"
if LOCAL_CFG.exists():
    merge(CFG, json.loads(LOCAL_CFG.read_text()))

HOUR, DAY = 3600, 86400
KEEP_DAYS = 8          # history needed for the 7-day heat baseline
CLUSTER_WINDOW = 2 * DAY  # reports further apart are treated as separate events
SHOW_WINDOW = 3 * DAY
TREND_DAYS = 90
STOPWORDS = set("with from that this says said after over have will into amid near more than "
                "about been their they what when where which were would could news live".split())


# ---------- fetching ----------

def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "geowatch/1.0 (self-hosted)"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read(10_000_000)


def parse_date(text):
    """Return a unix timestamp; falls back to now if the date is missing or odd."""
    for parser in (parsedate_to_datetime, lambda t: datetime.fromisoformat(t.replace("Z", "+00:00"))):
        try:
            dt = parser(text.strip())
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except (ValueError, TypeError, AttributeError):
            continue
    return int(time.time())


def clean(text):
    return html.unescape(re.sub(r"<[^>]+>", "", text or "")).strip()


def parse_feed(xml_bytes):
    """Yield (title, url, ts) from RSS 2.0, RSS 1.0 or Atom."""
    root = ET.fromstring(xml_bytes)
    for el in root.iter():
        el.tag = el.tag.split("}")[-1]  # drop XML namespaces
    for item in list(root.iter("item")) + list(root.iter("entry")):
        link = item.find("link")
        url = (link.text or link.get("href") or "").strip() if link is not None else ""
        date = item.findtext("pubDate") or item.findtext("published") or item.findtext("updated") or item.findtext("date")
        title = clean(item.findtext("title"))
        if title and url.startswith("http"):
            yield title, url, parse_date(date)


def fetch_gdelt():
    """News search across the web, filtered down to trusted domains only."""
    if not CFG.get("gdelt_enabled"):
        return
    params = {"query": CFG["gdelt_query"], "mode": "artlist", "format": "json",
              "maxrecords": 250, "timespan": "3h", "sort": "datedesc"}
    url = "https://api.gdeltproject.org/api/v2/doc/doc?" + urllib.parse.urlencode(params)
    try:
        raw = fetch(url)
    except urllib.error.HTTPError as e:
        if e.code != 429:
            raise
        time.sleep(20)  # GDELT rate-limits per IP; one retry is usually enough
        raw = fetch(url)
    data = json.loads(raw)
    for art in data.get("articles", []):
        source = CFG["trusted_domains"].get(art.get("domain", "").removeprefix("www."))
        if source:
            ts = datetime.strptime(art["seendate"], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            yield source, clean(art["title"]), art["url"], int(ts.timestamp())


def in_bbox(geometry, bbox):
    """True if the shape's first point lies inside [west, south, east, north]; no bbox keeps everything."""
    point = geometry["coordinates"]
    while isinstance(point[0], list):
        point = point[0]
    return not bbox or (bbox[0] <= point[0] <= bbox[2] and bbox[1] <= point[1] <= bbox[3])


SIGNALS = CFG.get("signals", {})
SIGNAL_REFRESH = 30 * 60  # sources update every few hours at most; no need to poll every scan
last_signals = 0
latest_quakes = []  # used by the personal zone alerts


def fetch_earthquakes():
    """USGS GeoJSON feed. Strong shallow quakes can also be large explosions, but this is not told apart."""
    quakes = []
    for f in json.loads(fetch(SIGNALS["earthquakes"]["url"]))["features"]:
        lon, lat, depth = f["geometry"]["coordinates"][:3]
        p = f["properties"]
        quakes.append({"lat": lat, "lon": lon, "depth": round(depth or 0), "mag": p["mag"],
                       "place": p.get("place") or "", "ts": p["time"] // 1000, "url": p.get("url", "")})
    return quakes


def fetch_fires():
    """NASA FIRMS satellite fire detections in the configured conflict areas (needs a free MAP_KEY)."""
    cfg = SIGNALS["fires"]
    fires = []
    for area, bbox in cfg["areas"].items():
        url = (f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{cfg['map_key']}/{cfg['source']}/"
               f"{','.join(map(str, bbox))}/{cfg['days']}")
        text = fetch(url).decode("utf-8", "replace")
        if not text.startswith("latitude"):  # FIRMS answers errors such as an invalid key as plain text
            raise ValueError(f"{area}: {text.strip()[:80]}")
        for row in csv.DictReader(io.StringIO(text)):
            frp = float(row.get("frp") or 0)
            if row.get("confidence") == "l" or frp < cfg["min_frp"]:  # VIIRS: l = low confidence
                continue
            when = datetime.strptime(row["acq_date"] + row["acq_time"].zfill(4), "%Y-%m-%d%H%M")
            fires.append({"lat": round(float(row["latitude"]), 4), "lon": round(float(row["longitude"]), 4),
                          "frp": round(frp, 1), "area": area,
                          "ts": int(when.replace(tzinfo=timezone.utc).timestamp())})
    fires.sort(key=lambda f: -f["frp"])
    return fires[:cfg["max_points"]]  # keep the page light; the strongest detections matter most


def update_signals(errors):
    """Write web/signals.json with the non-news layers. Each source fails on its own."""
    global last_signals
    if time.time() - last_signals < SIGNAL_REFRESH:
        return
    last_signals = time.time()
    out = {"updated": int(time.time()), "earthquakes": [], "fires": [], "fires_status": "off"}
    if SIGNALS.get("earthquakes", {}).get("enabled"):
        try:
            out["earthquakes"] = fetch_earthquakes()
        except Exception as e:
            errors.append(f"Earthquakes: {e}")
    fires_cfg = SIGNALS.get("fires", {})
    if fires_cfg.get("enabled"):
        if not fires_cfg.get("map_key"):
            out["fires_status"] = "no key"
        else:
            try:
                out["fires"] = fetch_fires()
                out["fires_status"] = "on"
            except Exception as e:
                out["fires_status"] = "failed"
                errors.append(f"Fires: {e}")
    (WEB / "signals.json").write_text(json.dumps(out))
    latest_quakes[:] = out["earthquakes"]


def update_frontlines():
    layers = []
    for fl in CFG["frontlines"]:
        geo = json.loads(fetch(fl["url"]))
        geo = geo[fl["geojson_key"]] if fl.get("geojson_key") else geo
        geo["features"] = [f for f in geo["features"] if f["geometry"]["type"] in ("Polygon", "MultiPolygon")
                           and in_bbox(f["geometry"], fl.get("bbox"))]
        layers.append({"name": fl["name"], "updated": int(time.time()), "geojson": geo})
    (WEB / "frontlines.json").write_text(json.dumps(layers))


# ---------- classification ----------

def keyword_re(words):
    return re.compile(r"\b(" + "|".join(map(re.escape, words)) + ")", re.I)


HOTSPOT_RE = [(h["id"], keyword_re(h["keywords"])) for h in CFG["hotspots"]]
CATEGORY_RE = [(name, keyword_re(words)) for name, words in CFG["categories"].items()]
INCIDENT_RE = keyword_re(CFG["incident_keywords"])
MILITARY_RE = keyword_re(CFG["military_context"])
SEVERE_RE = keyword_re(CFG["severe_keywords"])
# Whole-word match (unlike the keyword lists), so "star" does not hit "start" or "Starmer".
FEATURE_RE = re.compile(r"\b(" + "|".join(map(re.escape, CFG.get("feature_phrases", []))) + r")\b", re.I)
MOVEMENT_RE = dict(CATEGORY_RE)["movement"]


def word_re(words):
    """Whole-word match, for short words such as "eu" or "uk"."""
    return re.compile(r"\b(" + "|".join(map(re.escape, words)) + r")\b", re.I) if words else re.compile(r"(?!x)x")


POLITICS_RE = word_re(CFG.get("politics_keywords", []))
POLITICS_PLACES_RE = word_re(CFG.get("politics_countries", []))
POLITICS_EXCLUDE_RE = word_re(CFG.get("politics_exclude", []))
BIAS = CFG.get("source_bias", {})
LOCAL_POLITICS = {f["name"] for f in CFG["feeds"] if f.get("local_politics")}
STATUS_PAGES = {f["name"] for f in CFG["feeds"] if f.get("status_page")}  # provider status pages: every entry is an outage
OUTAGE_RE = word_re(CFG.get("outage_keywords", []))
STATUS_SKIP_RE = word_re(CFG.get("status_page_skip", []))
PROVIDER_RE = word_re(CFG.get("outage_providers", []))  # national outlets: all politics counts
VOTES = CFG.get("vote_results", {})
VOTE_CONTEXT_RE, VOTE_RESULT_RE, VOTE_PROJECTION_RE = (word_re(VOTES.get(k, [])) for k in ("context", "result", "projection"))
PLACE_RE = [(tuple(pos), keyword_re([name])) for name, pos in CFG.get("places", {}).items()]
HOTSPOT_POS = {h["id"]: (h["lat"], h["lon"]) for h in CFG["hotspots"]}


def first_match(patterns, text):
    return next((name for name, rx in patterns if rx.search(text)), None)


def classify(title, source=None):
    """Return (hotspot, category); (None, None) means the item is not relevant."""
    if source in STATUS_PAGES:
        return (None, None) if STATUS_SKIP_RE.search(title) else ("world", "outage")
    hotspot = first_match(HOTSPOT_RE, title)
    incident = "incident" if INCIDENT_RE.search(title) else None
    outage = "outage" if OUTAGE_RE.search(title) else None
    politics = "politics" if POLITICS_RE.search(title) and not POLITICS_EXCLUDE_RE.search(title) else None
    if hotspot:
        return hotspot, first_match(CATEGORY_RE, title) or incident or outage or politics
    if incident:
        return "world", incident    # security incidents anywhere in the world
    if outage:
        return "world", outage      # major IT, power and internet outages
    if MILITARY_RE.search(title) and MOVEMENT_RE.search(title):
        return "world", "movement"  # military movements outside the watched regions
    if politics and (source in LOCAL_POLITICS or POLITICS_PLACES_RE.search(title)):
        return "world", "politics"  # elections and votes in the countries chosen in politics_countries
    return None, None


def words(title):
    # 5-letter prefixes act as crude stems, so "strikes"/"strike" and "Russia"/"Russian" match
    return {w[:5] for w in re.findall(r"[a-z]{4,}", title.lower()) if w not in STOPWORDS}


# ---------- storage ----------

def db():
    DB.parent.mkdir(exist_ok=True)
    con = sqlite3.connect(DB)
    con.execute("CREATE TABLE IF NOT EXISTS items (url TEXT PRIMARY KEY, source TEXT, title TEXT,"
                " ts INTEGER, hotspot TEXT, category TEXT)")
    return con


def store(con, items):
    """Keep only items that are both in a hotspot and military/diplomatic."""
    now = int(time.time())
    for source, title, url, ts in items:
        hotspot, category = classify(title, source)
        if hotspot and category and ts > now - KEEP_DAYS * DAY:
            con.execute("INSERT OR IGNORE INTO items VALUES (?,?,?,?,?,?)",
                        (url, source, title, min(ts, now), hotspot, category))
    con.execute("DELETE FROM items WHERE ts < ?", (now - KEEP_DAYS * DAY,))
    con.commit()


# ---------- fact check (corroboration) ----------

EMB = CFG.get("embedding", {})


def embed_new(con, rows):
    """Get sentence embeddings for titles that have none yet (cached in the DB); return {url: unit vector}."""
    con.execute("CREATE TABLE IF NOT EXISTS embeddings (url TEXT PRIMARY KEY, vec TEXT)")
    have = {url for (url,) in con.execute("SELECT url FROM embeddings")}
    todo = [(row[2], row[1]) for row in rows if row[2] not in have]
    for i in range(0, len(todo), 16):
        batch = todo[i:i + 16]
        # num_thread: more threads than the container has cores makes CPU inference crawl
        body = json.dumps({"model": EMB["model"], "input": [title for _, title in batch],
                           "options": {"num_thread": EMB.get("threads", 2)}}).encode()
        req = urllib.request.Request(EMB["url"].rstrip("/") + "/api/embed", data=body,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            vectors = json.load(resp)["embeddings"]
        for (url, _), vec in zip(batch, vectors):
            norm = math.sqrt(sum(x * x for x in vec)) or 1.0  # unit length, so a dot product is the cosine
            con.execute("INSERT OR REPLACE INTO embeddings VALUES (?,?)",
                        (url, json.dumps([round(x / norm, 5) for x in vec])))
        con.commit()  # keep finished batches even if a later one times out
    con.execute("DELETE FROM embeddings WHERE url NOT IN (SELECT url FROM items)")
    con.commit()
    return {url: json.loads(vec) for url, vec in con.execute("SELECT url, vec FROM embeddings")}


def same_event(w, vec, c):
    """Meaning-based match when both titles have an embedding, otherwise shared-word matching."""
    if vec and c["vec"]:
        return sum(map(operator.mul, vec, c["vec"])) >= EMB["min_similarity"]
    shared = len(w & c["words"])
    return shared >= CFG["min_shared_words"] and shared >= 0.5 * min(len(w), len(c["words"]))


STATE_FUNDED = set(CFG.get("state_funded_sources", []))


def independent_count(titles):
    """Outlets running the identical headline are almost always republishing one wire report: count once."""
    return len({re.sub(r"[^a-z0-9]", "", t.lower()) for t in titles.values()})


def cluster(rows, vectors={}):
    """Group reports of the same event: same hotspot, close in time, similar titles."""
    clusters = []
    for source, title, url, ts, hotspot, category in rows:  # rows are oldest first
        w, vec = words(title), vectors.get(url)
        for c in clusters:
            if c["hotspot"] == hotspot and ts - c["first_ts"] < CLUSTER_WINDOW and same_event(w, vec, c):
                c["sources"].setdefault(source, url)
                c["titles"].setdefault(source, title)
                c["ts"] = ts
                break
        else:
            clusters.append({"title": title, "hotspot": hotspot, "category": category, "words": w, "vec": vec,
                             "first_ts": ts, "ts": ts, "sources": {source: url},
                             "titles": {source: title}})
    for c in clusters:
        c["independent"] = independent_count(c["titles"])
        # verified: enough independent reports, and not only from state-funded outlets
        c["verified"] = (c["independent"] >= CFG["min_sources"]
                         and any(name not in STATE_FUNDED for name in c["sources"]))
    return clusters


def heat(clusters, hotspot_id, now):
    """Compare verified events in the last 24h with the daily average of the 6 days before."""
    verified = [c for c in clusters if c["hotspot"] == hotspot_id and c["verified"]]
    recent = sorted((c for c in verified if c["first_ts"] > now - DAY), key=lambda c: -c["first_ts"])
    n24 = len(recent)
    base = sum(now - 7 * DAY < c["first_ts"] <= now - DAY for c in verified) / 6
    # rising needs at least 2 events, so a single story cannot flip a quiet region
    level = "boiling" if n24 >= 3 and n24 >= 2 * base else "rising" if n24 >= 2 and n24 > base else "stable"
    # the events behind the level, so the page can explain a change
    drivers = [{"title": c["title"], "url": next(iter(c["sources"].values())), "outlets": len(c["sources"]),
                "category": c["category"], "ts": c["first_ts"]} for c in recent[:6]]
    return {"n24": n24, "base": round(base, 1), "level": level, "drivers": drivers}


def to_event(c):
    """Map position: a known place named in the title, otherwise the hotspot centre."""
    place = first_match(PLACE_RE, c["title"])
    is_feature = bool(CFG.get("feature_phrases")) and bool(FEATURE_RE.search(c["title"]))
    lat, lon = place or HOTSPOT_POS.get(c["hotspot"], (None, None))  # worldwide events may have no position
    return {"title": c["title"], "hotspot": c["hotspot"], "category": c["category"], "ts": c["ts"],
            "verified": c["verified"],
            # important = shown in Live list and banner, alerts in the page; features/anniversaries never are
            "important": not is_feature and (c["verified"] or bool(SEVERE_RE.search(c["title"]))),
            "feature": is_feature,
            "lat": lat, "lon": lon, "exact": bool(place),
            "independent": c["independent"],
            "sources": [{"name": n, "url": u, "state": n in STATE_FUNDED, "title": c["titles"][n], "bias": BIAS.get(n)}
                        for n, u in c["sources"].items()]}


def local_settings():
    """Read config.local.json fresh on every scan, so zone and topic changes apply without a restart."""
    try:
        return json.loads(LOCAL_CFG.read_text())
    except (OSError, ValueError):
        return {}


def distance_km(lat1, lon1, lat2, lon2):
    """Great-circle distance (haversine)."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 12742 * math.asin(math.sqrt(a))


def active_zones(settings):
    today = datetime.now().strftime("%Y-%m-%d")
    return [z for z in settings.get("alert_zones", []) if not z.get("until") or z["until"] >= today]


def zone_matches(zone, title, lat, lon, exact):
    """A headline that names the zone, or an exactly placed event/quake inside the radius."""
    text = title.lower()
    if any(k in text for k in zone.get("keywords", [])):
        return True
    return exact and lat is not None and distance_km(zone["lat"], zone["lon"], lat, lon) <= zone["radius_km"]


def push(topic_url, title, message, link="", priority=3):
    """Publish to ntfy as JSON, which (unlike HTTP headers) carries umlauts and other UTF-8 safely."""
    base, topic = topic_url.rstrip("/").rsplit("/", 1)
    body = {"topic": topic, "title": title, "message": message, "priority": priority, "tags": ["warning"]}
    if link.startswith("http"):
        body["click"] = link
    req = urllib.request.Request(base + "/", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=10).close()


def is_final_vote_result(e):
    """Swiss referendum outcome: vote context + result verb, no projection wording.
    Reported by a Swiss national outlet, or confirmed by 2+ independent outlets."""
    local = any(s["name"] in LOCAL_POLITICS for s in e["sources"])
    swiss = local or re.search(r"\b(swiss|switzerland)\b", e["title"], re.I)
    return (e["category"] == "politics" and (local or e["verified"]) and not e["feature"] and swiss
            and any(VOTE_CONTEXT_RE.search(s["title"]) for s in e["sources"])
            and any(VOTE_RESULT_RE.search(s["title"]) for s in e["sources"])
            and not any(VOTE_PROJECTION_RE.search(s["title"]) for s in e["sources"]))


GENERIC_VOTE_WORDS = {"abstimmung", "abstimmungen", "abstimmungssonntag", "volksinitiative", "stimmvolk",
                      "stimmbevölkerung", "angenommen", "abgelehnt", "referendum", "switzerland", "initiative"}


def proposal_terms(title):
    """Long words name the proposal ("autobahnausbau", "motorway"); used to skip repeat reports of one result."""
    return {w for w in re.findall(r"[a-zäöüéèà]{8,}", title.lower()) if w not in GENERIC_VOTE_WORDS}


def vote_result_alerts(con, events, now):
    """Final results of Swiss popular votes, at most max_per_day pushes per day."""
    if not VOTES.get("enabled"):
        return []
    con.execute("CREATE TABLE IF NOT EXISTS vote_alerts (day TEXT, key TEXT PRIMARY KEY, title TEXT)")
    today = datetime.now().strftime("%Y-%m-%d")
    sent = [proposal_terms(t) for (t,) in con.execute("SELECT title FROM vote_alerts WHERE day = ?", (today,))]
    budget = VOTES.get("max_per_day", 6) - len(sent)
    alerts = []
    for e in sorted(events, key=lambda e: e["ts"]):
        key = "vote|" + e["sources"][0]["url"]
        if budget <= 0 or e["ts"] < now - 24 * HOUR or not is_final_vote_result(e):
            continue
        terms = proposal_terms(" ".join(s["title"] for s in e["sources"]))
        if any(terms & earlier for earlier in sent):  # same proposal already pushed today
            continue
        if con.execute("INSERT OR IGNORE INTO vote_alerts VALUES (?,?,?)", (today, key, e["title"])).rowcount:
            sent.append(terms)
            budget -= 1
            alerts.append((key, "Swiss vote: final result", f"{e['title']} ({', '.join(x['name'] for x in e['sources'])})",
                           e["sources"][0]["url"], 3))
    return alerts


def send_alerts(con, events, now):
    """Push via ntfy only what concerns the user's own zones (home, travel), like a personal Alertswiss."""
    settings = local_settings()
    topic, zones = settings.get("ntfy_url"), active_zones(settings)
    if not topic:
        return
    alerts = []  # (dedupe key, title, body, link, priority)
    if CFG.get("outage_push"):
        for e in events:  # confirmed outages at providers that matter to the user (cloud, M365, telcos)
            if e["category"] == "outage" and e["verified"] and not e["feature"] and e["ts"] > now - 6 * HOUR \
                    and any(PROVIDER_RE.search(s["title"]) for s in e["sources"]):
                alerts.append((f"outage|{e['sources'][0]['url']}", "Outage",
                               f"{e['title']} ({', '.join(x['name'] for x in e['sources'])})", e["sources"][0]["url"], 4))
    for e in events:
        if e["category"] in ("diplomacy", "politics") or e["feature"] or e["ts"] < now - 6 * HOUR:
            continue
        for z in zones:
            if zone_matches(z, e["title"], e["lat"], e["lon"], e["exact"]):
                note = f"{len(e['sources'])} sources" if e["verified"] else f"{e['sources'][0]['name']} only, unconfirmed"
                alerts.append((f"{z['name']}|{e['sources'][0]['url']}", f"{z['name']}: {e['category']}",
                               f"{e['title']} ({note})", e["sources"][0]["url"], 4 if e["important"] else 3))
    alerts += vote_result_alerts(con, events, now)
    for q in latest_quakes:
        for z in zones:
            if q["ts"] > now - 6 * HOUR and zone_matches(z, "", q["lat"], q["lon"], True):
                alerts.append((f"{z['name']}|quake|{q['ts']}", f"{z['name']}: earthquake M{q['mag']}",
                               f"{q['place']}, {q['depth']} km deep", q["url"], 4))
    con.execute("CREATE TABLE IF NOT EXISTS alerted (key TEXT PRIMARY KEY)")
    for key, title, body, link, priority in alerts:
        if con.execute("INSERT OR IGNORE INTO alerted VALUES (?)", (key,)).rowcount:
            try:
                push(topic, title, body, link, priority)
            except Exception as e:
                print("alert failed:", e, flush=True)
    con.commit()


def update_trends(con, clusters, now):
    """Store verified events per region and day. Recent days are recomputed, older ones stay as they were."""
    con.execute("CREATE TABLE IF NOT EXISTS daily (day TEXT, hotspot TEXT, verified INTEGER, total INTEGER,"
                " PRIMARY KEY (day, hotspot))")
    counts = {}
    for c in clusters:
        day = datetime.fromtimestamp(c["first_ts"], timezone.utc).strftime("%Y-%m-%d")
        entry = counts.setdefault((day, c["hotspot"]), [0, 0])
        entry[0] += c["verified"]
        entry[1] += 1
    oldest = datetime.fromtimestamp(now - (KEEP_DAYS - 1) * DAY, timezone.utc).strftime("%Y-%m-%d")
    for (day, hotspot), (verified, total) in counts.items():
        if day >= oldest:  # days at the edge of the item window are incomplete; keep their earlier count
            con.execute("INSERT OR REPLACE INTO daily VALUES (?,?,?,?)", (day, hotspot, verified, total))
    con.commit()
    since = datetime.fromtimestamp(now - TREND_DAYS * DAY, timezone.utc).strftime("%Y-%m-%d")
    trends = {}
    for day, hotspot, verified in con.execute("SELECT day, hotspot, verified FROM daily WHERE day >= ?", (since,)):
        trends.setdefault(hotspot, {})[day] = verified
    return trends


def build(con, errors):
    now = int(time.time())
    stored = con.execute("SELECT source, title, url, ts FROM items ORDER BY ts").fetchall()
    # Classify again on every build, so keyword changes in config.json also apply to items already stored.
    rows = [(source, title, url, ts, *classify(title, source)) for source, title, url, ts in stored]
    relevant = [row for row in rows if row[4] and row[5]]
    vectors = {}
    if EMB.get("enabled"):
        try:
            vectors = embed_new(con, relevant)
        except Exception as e:  # model host down: fall back to word matching for this scan
            errors.append(f"Embedding: {e}")
    clusters = cluster(relevant, vectors)
    events = [to_event(c) for c in clusters if c["ts"] > now - SHOW_WINDOW]
    feed_names = {f["name"] for f in CFG["feeds"]}
    data = {
        "updated": now,
        "errors": errors,
        "health": {  # shown in the status strip, so a silent failure on the server is visible
            "scan_interval_minutes": CFG["scan_interval_minutes"],
            "feeds_total": len(CFG["feeds"]),
            "feeds_failed": sum(e.split(":")[0] in feed_names for e in errors),
            "embedding": "off" if not EMB.get("enabled")
                         else "failed" if any(e.startswith("Embedding:") for e in errors) else "on",
        },
        "hotspots": [{k: h.get(k, "") for k in ("id", "name", "lat", "lon", "background")} | heat(clusters, h["id"], now)
                     for h in CFG["hotspots"]],
        "events": sorted(events, key=lambda e: -e["ts"]),
        "trends": update_trends(con, clusters, now),
    }
    (WEB / "data.json").write_text(json.dumps(data))
    send_alerts(con, data["events"], now)
    return data


# ---------- web server ----------

zone_lock = threading.Lock()


class Handler(SimpleHTTPRequestHandler):
    """Static files from web/, plus /api/zones to manage personal alert zones (token required)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(WEB), **kwargs)

    def reply(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def authorized(self):
        token = zone_store.load().get("admin_token")
        if not token:
            self.reply(403, {"error": "No token set. On the server run: zones.py token"})
            return False
        if not hmac.compare_digest(self.headers.get("X-Geowatch-Token", ""), token):
            self.reply(401, {"error": "Wrong token"})
            return False
        return True

    def do_GET(self):
        if self.path.split("?")[0] != "/api/zones":
            return super().do_GET()
        if self.authorized():
            self.reply(200, {"zones": zone_store.load().get("alert_zones", [])})

    def do_POST(self):
        if self.path != "/api/zones":
            return self.reply(404, {"error": "Not found"})
        if not self.authorized():
            return
        try:
            req = json.loads(self.rfile.read(min(int(self.headers.get("Content-Length", 0)), 10_000)))
            with zone_lock:
                cfg = zone_store.load()
                if req.get("action") == "remove":
                    zone_store.remove_zone(cfg, str(req["name"]))
                else:
                    name = str(req["name"]).strip()[:80]
                    if not name:
                        raise ValueError("Place name is empty")
                    until = req.get("until") or None
                    if until:
                        datetime.strptime(until, "%Y-%m-%d")  # rejects malformed dates
                    zone_store.add_zone(cfg, name, float(req.get("radius_km") or 50),
                                        str(req.get("keywords") or ""), until)
                zone_store.save(cfg)
            self.reply(200, {"zones": cfg["alert_zones"]})
        except SystemExit as e:  # geocode() reports "no place found" this way
            self.reply(400, {"error": str(e)})
        except Exception as e:
            self.reply(400, {"error": f"Could not save: {e}"})


# ---------- main ----------

def scan():
    items, errors = [], []
    def read(feed):
        try:
            return [(feed["name"], *entry) for entry in parse_feed(fetch(feed["url"]))], None
        except Exception as e:  # one broken source must not stop the scan
            return [], f"{feed['name']}: {e}"

    with ThreadPoolExecutor(max_workers=10) as pool:  # in parallel, so frequent scans stay short
        for found, error in pool.map(read, CFG["feeds"]):
            items += found
            if error:
                errors.append(error)
    update_signals(errors)
    for name, job in (("GDELT", lambda: items.extend(fetch_gdelt())), ("Frontlines", update_frontlines)):
        try:
            job()
        except Exception as e:
            errors.append(f"{name}: {e}")
    con = db()
    store(con, items)
    data = build(con, errors)
    con.close()
    print(f"{datetime.now():%F %T} scan: {len(items)} items, {len(data['events'])} events, "
          f"{sum(e['verified'] for e in data['events'])} verified, {len(errors)} source errors", flush=True)
    for e in errors:
        print("  !", e, flush=True)


def scan_loop():
    while True:
        try:
            scan()
        except Exception as e:
            print("scan failed:", e, flush=True)
        time.sleep(CFG["scan_interval_minutes"] * 60)


if __name__ == "__main__":
    if "--once" in sys.argv:
        scan()
    else:
        threading.Thread(target=scan_loop, daemon=True).start()
        print(f"serving on http://0.0.0.0:{CFG['port']}", flush=True)
        ThreadingHTTPServer(("0.0.0.0", CFG["port"]), Handler).serve_forever()
