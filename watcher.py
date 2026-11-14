#!/usr/bin/env python3
"""Geopolitics watcher.

Scans trusted feeds, keeps only military/diplomatic items in known hotspots,
groups reports of the same event, and marks an event "verified" only when
enough independent sources report it. Writes web/data.json and serves web/.

Usage:  python watcher.py          # scan loop + web server
        python watcher.py --once   # single scan, no server (cron/testing)
"""
import html
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
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).parent
WEB = ROOT / "web"
DB = ROOT / "data" / "watcher.db"
CFG = json.loads((ROOT / "config.json").read_text())
# Machine-specific settings (ntfy topic, thread count, ...) live in config.local.json, which git ignores,
# so pulling updates never overwrites them. Dict values are merged one level deep.
LOCAL_CFG = ROOT / "config.local.json"
if LOCAL_CFG.exists():
    for key, value in json.loads(LOCAL_CFG.read_text()).items():
        CFG[key] = {**CFG[key], **value} if isinstance(value, dict) and isinstance(CFG.get(key), dict) else value

HOUR, DAY = 3600, 86400
KEEP_DAYS = 8          # history needed for the 7-day heat baseline
CLUSTER_WINDOW = 2 * DAY  # reports further apart are treated as separate events
SHOW_WINDOW = 3 * DAY
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
MOVEMENT_RE = dict(CATEGORY_RE)["movement"]
PLACE_RE = [(tuple(pos), keyword_re([name])) for name, pos in CFG.get("places", {}).items()]
HOTSPOT_POS = {h["id"]: (h["lat"], h["lon"]) for h in CFG["hotspots"]}


def first_match(patterns, text):
    return next((name for name, rx in patterns if rx.search(text)), None)


def classify(title):
    """Return (hotspot, category); (None, None) means the item is not relevant."""
    hotspot = first_match(HOTSPOT_RE, title)
    incident = "incident" if INCIDENT_RE.search(title) else None
    if hotspot:
        return hotspot, first_match(CATEGORY_RE, title) or incident
    if incident:
        return "world", incident    # security incidents anywhere in the world
    if MILITARY_RE.search(title) and MOVEMENT_RE.search(title):
        return "world", "movement"  # military movements outside the watched regions
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
        hotspot, category = classify(title)
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


def cluster(rows, vectors={}):
    """Group reports of the same event: same hotspot, close in time, similar titles."""
    clusters = []
    for source, title, url, ts, hotspot, category in rows:  # rows are oldest first
        w, vec = words(title), vectors.get(url)
        for c in clusters:
            if c["hotspot"] == hotspot and ts - c["first_ts"] < CLUSTER_WINDOW and same_event(w, vec, c):
                c["sources"].setdefault(source, url)
                c["ts"] = ts
                break
        else:
            clusters.append({"title": title, "hotspot": hotspot, "category": category, "words": w, "vec": vec,
                             "first_ts": ts, "ts": ts, "sources": {source: url}})
    for c in clusters:
        c["verified"] = len(c["sources"]) >= CFG["min_sources"]
    return clusters


def heat(clusters, hotspot_id, now):
    """Compare verified events in the last 24h with the daily average of the 6 days before."""
    mine = [c["first_ts"] for c in clusters if c["hotspot"] == hotspot_id and c["verified"]]
    n24 = sum(ts > now - DAY for ts in mine)
    base = sum(now - 7 * DAY < ts <= now - DAY for ts in mine) / 6
    level = "boiling" if n24 >= 3 and n24 >= 2 * base else "rising" if n24 > base else "stable"
    return {"n24": n24, "base": round(base, 1), "level": level}


def to_event(c):
    """Map position: a known place named in the title, otherwise the hotspot centre."""
    place = first_match(PLACE_RE, c["title"])
    lat, lon = place or HOTSPOT_POS.get(c["hotspot"], (None, None))  # worldwide events may have no position
    return {"title": c["title"], "hotspot": c["hotspot"], "category": c["category"], "ts": c["ts"],
            "verified": c["verified"],
            "important": c["verified"] or bool(SEVERE_RE.search(c["title"])),  # shown in the live ticker
            "lat": lat, "lon": lon, "exact": bool(place),
            "sources": [{"name": n, "url": u} for n, u in c["sources"].items()]}


def send_alerts(con, events, hotspots, now):
    """Optional push via ntfy: new verified events and hotspots that turn boiling. Each is sent once."""
    if not CFG.get("ntfy_url"):
        return
    names = {h["id"]: h["name"] for h in hotspots}
    alerts = [(e["sources"][0]["url"], f"{names.get(e['hotspot'], 'Worldwide')}: {e['category']}", e["title"])
              for e in events if e["verified"] and e["ts"] > now - 6 * HOUR]
    alerts += [(f"boiling:{h['id']}:{now // DAY}", "Boiling pot", f"{h['name']}: {h['n24']} verified events in 24h")
               for h in hotspots if h["level"] == "boiling"]
    con.execute("CREATE TABLE IF NOT EXISTS alerted (key TEXT PRIMARY KEY)")
    for key, title, body in alerts:
        if con.execute("INSERT OR IGNORE INTO alerted VALUES (?)", (key,)).rowcount:
            try:
                req = urllib.request.Request(CFG["ntfy_url"], data=body.encode(), headers={"Title": title})
                urllib.request.urlopen(req, timeout=10).close()
            except Exception as e:
                print("alert failed:", e, flush=True)
    con.commit()


def build(con, errors):
    now = int(time.time())
    stored = con.execute("SELECT source, title, url, ts FROM items ORDER BY ts").fetchall()
    # Classify again on every build, so keyword changes in config.json also apply to items already stored.
    rows = [(source, title, url, ts, *classify(title)) for source, title, url, ts in stored]
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
    }
    (WEB / "data.json").write_text(json.dumps(data))
    send_alerts(con, data["events"], data["hotspots"], now)
    return data


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
        handler = partial(SimpleHTTPRequestHandler, directory=str(WEB))
        print(f"serving on http://0.0.0.0:{CFG['port']}", flush=True)
        ThreadingHTTPServer(("0.0.0.0", CFG["port"]), handler).serve_forever()
