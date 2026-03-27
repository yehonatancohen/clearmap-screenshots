"""
Quick unit test for the geographic clustering logic in main.py.
No Firebase, Telegram, or Playwright needed — just polygons.json.

Usage:
    python test_clustering.py
"""
import sys
import io
# Force UTF-8 stdout so Hebrew city names and arrows print correctly on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import json
import math
import time
import itertools
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

# ── Inline the pure functions from main.py (no heavy deps needed) ────────────

GEO_CLUSTER_KM = 50.0

def _haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def _compute_centroid(polygon_coords):
    n = len(polygon_coords)
    if n == 0:
        return (0.0, 0.0)
    return (sum(p[0] for p in polygon_coords) / n, sum(p[1] for p in polygon_coords) / n)

def _cluster_cities(city_names, centroids, threshold_km=GEO_CLUSTER_KM):
    if not city_names:
        return []
    if not centroids:
        return [set(city_names)]
    known = [c for c in city_names if c in centroids]
    unknown = [c for c in city_names if c not in centroids]
    visited = set()
    clusters = []
    for start in known:
        if start in visited:
            continue
        cluster = set()
        queue = [start]
        while queue:
            city = queue.pop()
            if city in visited:
                continue
            visited.add(city)
            cluster.add(city)
            lat1, lon1 = centroids[city]
            for other in known:
                if other not in visited:
                    lat2, lon2 = centroids[other]
                    if _haversine_km(lat1, lon1, lat2, lon2) <= threshold_km:
                        queue.append(other)
        clusters.append(cluster)
    for city in unknown:
        clusters.append({city})
    return clusters

@dataclass
class GroupState:
    group_id: int
    city_names: set
    last_broadcast_snapshot: dict
    message_ids: dict
    last_broadcast_time: float

_group_id_counter = itertools.count(1)
_STATUS_PRIORITY = {"pre_alert": 1, "alert": 2, "uav": 2, "terrorist": 2}

def _match_clusters_to_states(new_clusters, existing_states):
    triples = []
    for ci, cluster in enumerate(new_clusters):
        for sid, state in existing_states.items():
            overlap = len(cluster & state.city_names)
            if overlap > 0:
                triples.append((overlap, ci, sid))
    triples.sort(key=lambda x: x[0], reverse=True)
    assigned_clusters = set()
    assigned_states = set()
    matched = {}
    for _overlap, ci, sid in triples:
        if ci not in assigned_clusters and sid not in assigned_states:
            matched[ci] = (new_clusters[ci], existing_states[sid])
            assigned_clusters.add(ci)
            assigned_states.add(sid)
    unmatched_new = [new_clusters[ci] for ci in range(len(new_clusters)) if ci not in assigned_clusters]
    orphaned_states = [existing_states[sid] for sid in existing_states if sid not in assigned_states]
    return matched, unmatched_new, orphaned_states

def _compute_group_diff(city_by_status, state):
    all_snapshot_cities = set()
    for cities_set in state.last_broadcast_snapshot.values():
        all_snapshot_cities.update(cities_set)
    new_same = set()
    upgraded = defaultdict(set)
    for city, new_status in city_by_status.items():
        if city not in all_snapshot_cities:
            new_same.add(city)
        else:
            old_status = None
            for snap_status, snap_cities in state.last_broadcast_snapshot.items():
                if city in snap_cities:
                    old_status = snap_status
                    break
            if old_status and old_status != new_status:
                old_prio = _STATUS_PRIORITY.get(old_status, 0)
                new_prio = _STATUS_PRIORITY.get(new_status, 0)
                if new_prio > old_prio:
                    upgraded[new_status].add(city)
    return new_same, dict(upgraded)

# ── Load centroids ────────────────────────────────────────────────────────────

POLYGONS_PATH = Path(__file__).parent.parent / "clear-map-backend" / "polygons.json"

def load_centroids():
    if not POLYGONS_PATH.exists():
        print(f"ERROR: polygons.json not found at {POLYGONS_PATH}")
        sys.exit(1)
    with open(POLYGONS_PATH, encoding="utf-8") as f:
        data = json.load(f)
    return {name: _compute_centroid(entry["polygon"]) for name, entry in data.items() if entry.get("polygon")}

# ── Test helpers ─────────────────────────────────────────────────────────────

def check(label, got, expected):
    ok = got == expected
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}]  {label}: got {got}, expected {expected}")
    return ok

# ── Tests ─────────────────────────────────────────────────────────────────────

def test_clustering(centroids):
    print("\n=== Geographic Clustering Tests ===\n")
    total = passed = 0

    # 1. Haifa + Tel Aviv without a chain — should be 2 clusters (~80 km apart)
    cities = ["חיפה - מפרץ", "תל אביב - מרכז העיר"]
    missing = [c for c in cities if c not in centroids]
    if missing:
        print(f"  [SKIP]  Skipping test 1 -- cities not in polygons.json: {missing}")
    else:
        clusters = _cluster_cities(cities, centroids)
        total += 1
        if check("Haifa + TLV (no chain) → 2 clusters", len(clusters), 2):
            passed += 1

    # 2. Coastal chain: Haifa → Hadera → Netanya → TLV — should be 1 cluster
    coastal_chain = ["חיפה - מפרץ", "חדרה - מרכז", "נתניה - מרכז" if "נתניה - מרכז" in centroids else "נתניה - מזרח", "תל אביב - מרכז העיר"]
    coastal_chain = [c for c in coastal_chain if c in centroids]
    if len(coastal_chain) < 3:
        print(f"  [SKIP]  Skipping test 2 -- not enough cities found in polygons.json")
    else:
        clusters = _cluster_cities(coastal_chain, centroids)
        total += 1
        if check(f"Coastal chain ({' -> '.join(coastal_chain)}) -> 1 cluster", len(clusters), 1):
            passed += 1

    # 3. North + South (far apart) — should be 2 clusters
    north_south = ["קריית שמונה", "אשקלון - צפון"]
    missing = [c for c in north_south if c not in centroids]
    if missing:
        print(f"  [SKIP]  Skipping test 3 -- cities not in polygons.json: {missing}")
    else:
        clusters = _cluster_cities(north_south, centroids)
        total += 1
        if check("North (Kiryat Shmona) + South (Ashkelon) → 2 clusters", len(clusters), 2):
            passed += 1

    # 4. Empty input
    clusters = _cluster_cities([], centroids)
    total += 1
    if check("Empty city list → 0 clusters", len(clusters), 0):
        passed += 1

    # 5. Single city
    clusters = _cluster_cities(["תל אביב - מרכז העיר"], centroids)
    total += 1
    if check("Single city → 1 cluster", len(clusters), 1):
        passed += 1

    # 6. City not in polygons → singleton cluster
    clusters = _cluster_cities(["עיר_לא_קיימת"], centroids)
    total += 1
    if check("Unknown city → 1 singleton cluster", len(clusters), 1):
        passed += 1

    # 7. No centroids → single group fallback
    clusters = _cluster_cities(["חיפה - מפרץ", "תל אביב - מרכז העיר"], {})
    total += 1
    if check("No centroids → single group fallback", len(clusters), 1):
        passed += 1

    # 8. Print actual distances for reference
    if "חיפה - מפרץ" in centroids and "תל אביב - מרכז העיר" in centroids:
        lat1, lon1 = centroids["חיפה - מפרץ"]
        lat2, lon2 = centroids["תל אביב - מרכז העיר"]
        dist = _haversine_km(lat1, lon1, lat2, lon2)
        print(f"\n  [INFO] Haifa Bay <-> TLV center: {dist:.1f} km (cluster threshold: {GEO_CLUSTER_KM:.0f} km)")

    if "חיפה - מפרץ" in centroids and "חדרה - מרכז" in centroids:
        lat1, lon1 = centroids["חיפה - מפרץ"]
        lat2, lon2 = centroids["חדרה - מרכז"]
        dist = _haversine_km(lat1, lon1, lat2, lon2)
        print(f"  [INFO] Haifa Bay <-> Hadera center: {dist:.1f} km")

    if "חדרה - מרכז" in centroids and "נתניה - מזרח" in centroids:
        lat1, lon1 = centroids["חדרה - מרכז"]
        lat2, lon2 = centroids["נתניה - מזרח"]
        dist = _haversine_km(lat1, lon1, lat2, lon2)
        print(f"  [INFO] Hadera center <-> Netanya east: {dist:.1f} km")

    if "נתניה - מזרח" in centroids and "תל אביב - מרכז העיר" in centroids:
        lat1, lon1 = centroids["נתניה - מזרח"]
        lat2, lon2 = centroids["תל אביב - מרכז העיר"]
        dist = _haversine_km(lat1, lon1, lat2, lon2)
        print(f"  [INFO] Netanya east <-> TLV center: {dist:.1f} km")

    print(f"\n  {passed}/{total} passed")
    return passed, total


def test_matching_and_diff():
    print("\n=== Group Matching & Diff Tests ===\n")
    total = passed = 0
    now = time.time()

    # Test 1: New cities at same status → edit
    state = GroupState(
        group_id=1,
        city_names={"חיפה - מפרץ", "עכו"},
        last_broadcast_snapshot={"pre_alert": frozenset({"חיפה - מפרץ", "עכו"})},
        message_ids={"-100123": 42},
        last_broadcast_time=now - 10,
    )
    city_by_status = {"חיפה - מפרץ": "pre_alert", "עכו": "pre_alert", "נהריה": "pre_alert"}
    new_same, upgraded = _compute_group_diff(city_by_status, state)
    total += 1
    if check("New pre_alert city → new_same contains it", "נהריה" in new_same, True):
        passed += 1
    total += 1
    if check("No upgrades when same status", len(upgraded), 0):
        passed += 1

    # Test 2: Status upgrade (pre_alert → alert) → new message
    state2 = GroupState(
        group_id=2,
        city_names={"תל אביב - מרכז העיר", "בת ים"},
        last_broadcast_snapshot={"pre_alert": frozenset({"תל אביב - מרכז העיר", "בת ים"})},
        message_ids={"-100456": 99},
        last_broadcast_time=now - 60,
    )
    city_by_status2 = {"תל אביב - מרכז העיר": "alert", "בת ים": "pre_alert"}
    new_same2, upgraded2 = _compute_group_diff(city_by_status2, state2)
    total += 1
    if check("TLV upgraded pre_alert→alert detected", "תל אביב - מרכז העיר" in upgraded2.get("alert", set()), True):
        passed += 1
    total += 1
    if check("Bat Yam still pre_alert → not in upgraded", "בת ים" not in upgraded2.get("alert", set()), True):
        passed += 1

    # Test 3: Matching — existing North group matched to new North cluster
    north_state = GroupState(
        group_id=1,
        city_names={"חיפה - מפרץ", "עכו"},
        last_broadcast_snapshot={"pre_alert": frozenset({"חיפה - מפרץ", "עכו"})},
        message_ids={},
        last_broadcast_time=now,
    )
    center_state = GroupState(
        group_id=2,
        city_names={"תל אביב - מרכז העיר"},
        last_broadcast_snapshot={"alert": frozenset({"תל אביב - מרכז העיר"})},
        message_ids={},
        last_broadcast_time=now,
    )
    new_clusters = [
        {"חיפה - מפרץ", "עכו", "נהריה"},   # North (expanded)
        {"תל אביב - מרכז העיר", "חולון"},  # Center (expanded)
    ]
    existing = {1: north_state, 2: center_state}
    matched, unmatched, orphaned = _match_clusters_to_states(new_clusters, existing)

    total += 1
    if check("Both clusters matched (no unmatched)", len(unmatched), 0):
        passed += 1
    total += 1
    if check("No orphaned states", len(orphaned), 0):
        passed += 1
    total += 1
    if check("2 matched clusters", len(matched), 2):
        passed += 1

    # Test 4: New cluster → unmatched
    completely_new = [{"באר שבע", "אשקלון - צפון"}]
    matched2, unmatched2, orphaned2 = _match_clusters_to_states(completely_new, existing)
    total += 1
    if check("Brand-new cluster → unmatched", len(unmatched2), 1):
        passed += 1

    # Test 5: Cleared alerts → orphaned state
    empty_clusters = [{"תל אביב - מרכז העיר", "חולון"}]
    matched3, unmatched3, orphaned3 = _match_clusters_to_states(empty_clusters, existing)
    total += 1
    if check("North alerts cleared → north state orphaned", len(orphaned3), 1):
        passed += 1

    print(f"\n  {passed}/{total} passed")
    return passed, total


if __name__ == "__main__":
    print("Loading centroids from polygons.json...")
    centroids = load_centroids()
    print(f"Loaded {len(centroids)} city centroids.\n")

    p1, t1 = test_clustering(centroids)
    p2, t2 = test_matching_and_diff()

    total_pass = p1 + p2
    total_tests = t1 + t2
    print(f"\n{'='*40}")
    print(f"Total: {total_pass}/{total_tests} tests passed")
    if total_pass < total_tests:
        sys.exit(1)
