# Geowatch

Self-hosted geopolitics watcher. Python standard library only, no API keys, no LLM calls.

## Run

```sh
docker compose up -d --build     # then open http://<host>:8080
```

Without Docker: `python3 watcher.py` (Python 3.10+). Single scan for cron/testing: `python3 watcher.py --once`.

## How it works

1. **Scan** (every `scan_interval_minutes`): RSS/Atom feeds in `config.json` plus one GDELT news query,
   filtered to `trusted_domains`.
2. **Filter**: an item is kept only if its title matches a hotspot keyword AND a category keyword
   (movement / combat / diplomacy).
3. **Fact check**: reports of the same event are grouped (same hotspot, within 48h, similar titles).
   An event is `verified` only if `min_sources` independent sources report it. Unverified events are
   hidden by default.
4. **Boiling pots**: verified events in the last 24h vs the daily average of the previous 6 days.
   `boiling` = at least 3 and at least double the baseline, `rising` = above baseline.
5. **Frontlines**: GeoJSON layers listed under `frontlines` are redrawn on each scan.

## Limits (read these)

- "Verified" means corroborated by several trusted outlets. It does not prove the event happened;
  outlets can all repeat the same wire report or the same official claim.
- Troop movements are those reported in the news, placed at the hotspot's map position. This is not
  live unit tracking.
- Only Ukraine has a frontline layer (DeepState, unofficial endpoint, may change). Add other
  GeoJSON sources under `frontlines` if you find them.
- Matching is by title keywords, so expect some misses and some noise. Tune `hotspots`,
  `categories`, `min_shared_words` in `config.json`.
- The first days have a thin baseline, so heat levels settle after about a week.

## Maintenance

- Failing sources show in the page header and in `docker compose logs`. Fix or remove the URL in
  `config.json`, then `docker compose restart`.
- Add a source: one line in `feeds`. If it should also count via GDELT, add its domain to
  `trusted_domains` with the same name so it is not counted twice.
- History lives in `data/watcher.db` (8 days). Safe to delete; it rebuilds from the feeds.
- No authentication. Keep it on the LAN or behind your VPN/reverse proxy.

## Map markers and alerts

- Small dots are events, coloured by category (blue = movement). Solid = verified, dashed = single source
  (only with "show unverified"). Position comes from `places` in `config.json` when the title names a
  known place, otherwise the hotspot centre. Add places as `"name": [lat, lon]`.
- In-page alerts: new verified events and hotspots turning boiling pop up bottom-left while the page is
  open. "Enable alerts" adds desktop notifications (works on `localhost` or HTTPS only).
- Push alerts with the page closed: set `ntfy_url` to an ntfy topic, e.g. `https://ntfy.sh/<long-random-name>`
  or your own ntfy server, and subscribe in the ntfy app. Anyone who knows the topic name can read it.

## Worldwide incidents and movements

- Outside the watched regions, an item is kept if its title matches `incident_keywords` (bombs found,
  sabotage, terror, cyberattack, ...) or a movement keyword together with a `military_context` word.
  These show as region "Worldwide"; they get a map dot only if the title names a place from `places`.
- The Filters panel (top right) filters list and map by category, region, period, verification and text.

## Optional: embedding model for better event merging

Word matching misses the same event under different headlines. With `embedding.enabled` = true, titles
are sent to an Ollama server (`embedding.url`, CPU is enough) and merged by meaning (cosine similarity
>= `min_similarity`). Vectors are cached in the DB, so only new titles are embedded. If the server is
unreachable, that scan falls back to word matching and "Embedding" shows as a failing source.
Setup: `ollama pull all-minilm`. Raise `min_similarity` if unrelated events merge, lower it if duplicates remain.

## Local settings and auto-update

- Put machine-specific settings in `config.local.json` (not tracked by git), e.g.
  `{"ntfy_url": "https://ntfy.sh/<topic>", "embedding": {"threads": 2}}`. Keys override `config.json`.
- On the server, `deploy/geowatch-update.timer` runs `deploy/update.sh` every 15 minutes: it does
  `git pull --ff-only` and restarts Geowatch only when new commits arrived.
  Check it with `systemctl list-timers geowatch-update.timer` and `journalctl -u geowatch-update`.
- Anything pushed to the repository's main branch runs on the server within 15 minutes.

## Hard-signal layers

- **Earthquakes:** USGS feed, magnitude 4.5+ of the last 24 hours, no key needed.
- **Satellite fire detections:** NASA FIRMS (VIIRS), last 24 hours, only inside `signals.fires.areas`.
  Needs a free MAP_KEY from https://firms.modaps.eosdis.nasa.gov/api/map_key/ , set in `config.local.json`:
  `{"signals": {"fires": {"map_key": "<key>"}}}`. Fires are not proof of attacks: wildfires, farming and
  gas flares look the same. Iran and the Gulf are left out on purpose because of permanent gas flares.
- Both refresh every 30 minutes and fail independently; errors show in the header.

## Personal push alerts (zones)

Push notifications via ntfy go out only for your own zones, like a personal Alertswiss:
security incidents, combat and military movements whose headline names a zone keyword or whose exact
position lies inside the zone radius, plus earthquakes M4.5+ inside the radius. Diplomacy and the rest of
the world are never pushed. Each alert is sent once; unconfirmed single-source reports are labelled.

Manage zones on the server (as root): `python3 /opt/geowatch/zones.py list|add|remove|topic|test`.
Travel zones take `--until YYYY-MM-DD` and switch off by themselves. Changes apply at the next scan.
Coordinates are looked up by name via OpenStreetMap Nominatim. Settings are stored in config.local.json.
The in-page alerts (toasts, desktop notifications) are unchanged and still cover everything important.

Zones can also be managed in the web page ("My alert zones"). Create the token once on the server with
`python3 /opt/geowatch/zones.py token`; the page asks for it on first use and remembers it in that browser.
The ntfy topic can only be changed on the server.

## Background stories

Headlines matching `feature_phrases` (anniversaries, interviews, history pieces, analysis, ...) stay in the
event list, marked "background story", but are never treated as important: no Live list, no banner, no alerts.
Phrases match whole words only.

## Independence, trends, zone view

- **Verified** needs `min_sources` independent reports. Identical headlines from different outlets count once
  (they are almost always the same wire report). Outlets in `state_funded_sources` count, but cannot verify an
  event on their own. That list is a judgement call; edit it in `config.json`.
- **Trends:** verified events per region and day are kept for 90 days (table `daily`), shown as a 60-day chart
  when a region is selected. History starts on the day this version was installed.
- **My alert zones** in the Region filter shows only events matching your zones, with the same rule as the
  phone push. Zones are drawn as dashed circles once the page has your token.
- **View new** (status line) shows only events that arrived since your last visit.

## Politics and coverage across the spectrum

- **Politics** category: elections, votes, referendums, parliaments and coalitions, in watched regions and in the
  countries listed in `politics_countries`. `politics_exclude` drops Federal Council, cantonal and municipal
  elections. Politics is never pushed to the phone.
- **Coverage panel:** click an event to see every outlet that reported it, its political rating and its own headline,
  plus a left/center/right bar and a hint when one side of the spectrum did not cover it.
- Ratings live in `source_bias` (config.json): AllSides where available, otherwise Media Bias/Fact Check (MBFC).
  The AllSides values come from an older community snapshot and are marked as such; MBFC values are from its
  dataset as of 14 Nov 2026. Ratings describe the outlet, not the article. Most feeds are centre to lean-left,
  so a missing right-leaning view is common and partly reflects the feed selection.
