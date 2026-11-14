#!/usr/bin/env python3
"""Manage personal alert zones and the ntfy topic in config.local.json.

Run on the server as root, e.g.:
  python3 /opt/geowatch/zones.py list
  python3 /opt/geowatch/zones.py add "Zürich" --radius 40 --keywords zurich,zürich,switzerland,swiss
  python3 /opt/geowatch/zones.py add "Mallorca" --radius 80 --until 2026-12-20
  python3 /opt/geowatch/zones.py remove "Mallorca"
  python3 /opt/geowatch/zones.py topic        # asks for the ntfy topic URL, hidden
  python3 /opt/geowatch/zones.py test         # sends a test push
  python3 /opt/geowatch/zones.py token        # creates the token for editing zones in the web page
Changes apply at the next scan, no restart needed.
"""
import argparse
import getpass
import json
import os
import pwd
import secrets
import urllib.parse
import urllib.request
from pathlib import Path

LOCAL = Path(__file__).parent / "config.local.json"


def load():
    return json.loads(LOCAL.read_text()) if LOCAL.exists() else {}


def save(cfg):
    LOCAL.write_text(json.dumps(cfg, indent=2, ensure_ascii=False))
    if os.geteuid() == 0:  # run as root: hand the file to the service user (if it exists), keep it private
        try:
            user = pwd.getpwnam("geowatch")
            os.chown(LOCAL, user.pw_uid, user.pw_gid)
        except KeyError:
            pass
    LOCAL.chmod(0o600)


def geocode(name):
    """Look up coordinates with OpenStreetMap Nominatim (one request, as its usage policy allows)."""
    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode({"q": name, "format": "json", "limit": 1})
    req = urllib.request.Request(url, headers={"User-Agent": "geowatch/1.0 (self-hosted)"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        hits = json.load(resp)
    if not hits:
        raise SystemExit(f'No place found for "{name}". Pass --lat and --lon instead.')
    print(f"Found: {hits[0]['display_name']}")
    return round(float(hits[0]["lat"]), 4), round(float(hits[0]["lon"]), 4)


def add_zone(cfg, name, radius_km=50, keywords="", until=None, lat=None, lon=None):
    """Add or replace a zone; looks up coordinates by name unless given. Returns the zone."""
    if lat is None or lon is None:
        lat, lon = geocode(name)
    words = {name.lower(), *(w.strip().lower() for w in keywords.split(",") if w.strip())}
    zones = cfg.setdefault("alert_zones", [])
    zones[:] = [z for z in zones if z["name"].lower() != name.lower()]  # adding again replaces
    zone = {"name": name, "lat": lat, "lon": lon, "radius_km": radius_km, "keywords": sorted(words), "until": until}
    zones.append(zone)
    return zone


def remove_zone(cfg, name):
    """Returns True if a zone was removed."""
    zones = cfg.setdefault("alert_zones", [])
    before = len(zones)
    zones[:] = [z for z in zones if z["name"].lower() != name.lower()]
    return len(zones) < before


def main():
    ap = argparse.ArgumentParser(description="Personal alert zones for Geowatch")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    add = sub.add_parser("add")
    add.add_argument("name")
    add.add_argument("--radius", type=float, default=50, help="km, default 50")
    add.add_argument("--keywords", default="", help="comma-separated words that match headlines (name is always included)")
    add.add_argument("--until", help="YYYY-MM-DD, zone switches off after this day (for travel)")
    add.add_argument("--lat", type=float)
    add.add_argument("--lon", type=float)
    rm = sub.add_parser("remove")
    rm.add_argument("name")
    sub.add_parser("topic")
    sub.add_parser("test")
    sub.add_parser("token")
    args = ap.parse_args()
    cfg = load()
    zones = cfg.setdefault("alert_zones", [])  # noqa: used by list

    if args.cmd == "list":
        for z in zones:
            print(f"{z['name']}: {z['radius_km']} km around {z['lat']},{z['lon']}, keywords {z['keywords']}, "
                  f"until {z.get('until') or 'always'}")
        print("ntfy topic:", "set" if cfg.get("ntfy_url") else "NOT SET (run: zones.py topic)")
        return
    if args.cmd == "add":
        add_zone(cfg, args.name, args.radius, args.keywords, args.until, args.lat, args.lon)
        print(f"Zone {args.name} saved.")
    elif args.cmd == "remove":
        print("Removed." if remove_zone(cfg, args.name) else "No zone with that name.")
    elif args.cmd == "topic":
        url = getpass.getpass("ntfy topic URL (e.g. https://ntfy.sh/<long-random-name>): ").strip()
        if not url.startswith("http"):
            raise SystemExit("Not a URL, nothing changed.")
        cfg["ntfy_url"] = url
        print("Topic saved.")
    elif args.cmd == "token":
        cfg["admin_token"] = secrets.token_urlsafe(18)
        print("Token for the web page (shown once, enter it when the page asks):")
        print(cfg["admin_token"])
    elif args.cmd == "test":
        if not cfg.get("ntfy_url"):
            raise SystemExit("No topic set. Run: zones.py topic")
        base, topic = cfg["ntfy_url"].rstrip("/").rsplit("/", 1)
        body = json.dumps({"topic": topic, "title": "Geowatch test", "message": "Alerts for your zones arrive here.",
                           "tags": ["white_check_mark"]}).encode()
        urllib.request.urlopen(urllib.request.Request(base + "/", data=body,
                               headers={"Content-Type": "application/json"}), timeout=10).close()
        print("Test push sent.")
        return
    save(cfg)


if __name__ == "__main__":
    main()
