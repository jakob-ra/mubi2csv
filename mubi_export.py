import requests
import time
import csv as _csv
import sys
import argparse
import uuid
from typing import Optional, Dict, Any, List
import pandas as pd
from ast import literal_eval

BASE = "https://api.mubi.com/v4/users/{user_id}/{endpoint}"

def debug_print(debug: bool, *args, **kwargs):
    if debug:
        print(*args, **kwargs)


def get_json_with_retries(session: requests.Session, url: str, params: dict, headers: dict,
                          max_retries: int = 4, debug: bool = False):
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=20)
            debug_print(debug, f"[HTTP {resp.status_code}] {resp.url}")
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError:
                    raise RuntimeError("Response not JSON (200).")
            # surface 4xx non-retry errors
            if 400 <= resp.status_code < 500 and resp.status_code != 429:
                # try to show JSON error if any
                try:
                    msg = resp.json()
                except Exception:
                    msg = resp.text[:400]
                raise requests.HTTPError(f"Client error {resp.status_code}: {msg}", response=resp)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", backoff))
                debug_print(debug, f"Rate limited (429). Waiting {wait}s...")
                time.sleep(wait)
            else:
                debug_print(debug, f"Server error {resp.status_code}. Retrying in {backoff}s...")
                time.sleep(backoff)
        except requests.RequestException as e:
            if attempt == max_retries:
                raise
            debug_print(debug, f"RequestException: {e}. Retrying in {backoff}s...")
            time.sleep(backoff)
        backoff *= 2
    raise RuntimeError("Failed to fetch after retries")


def flatten_json(obj, parent_key='', sep='.'):
    """
    Recursively flatten JSON dict into a flat dict with dot-separated keys.
    Lists are converted to comma-separated strings if elements are simple types.
    Skips media/image/video fields.
    """
    flat = {}
    media_keys = {'stills', 'still_url', 'portrait_image', 'trailer_url', 'artworks', 'optimised_trailers'}

    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in media_keys:
                continue
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            flat.update(flatten_json(v, new_key, sep=sep))
    elif isinstance(obj, list):
        # simple list (str/int) -> comma-separated string
        if all(isinstance(el, (str, int, float, bool)) for el in obj):
            flat[parent_key] = ', '.join(map(str, obj))
        else:
            # complex list -> convert each element to string and join
            flat[parent_key] = ', '.join([str(el) for el in obj])
    else:
        flat[parent_key] = obj
    return flat


def paginate_api(session: requests.Session, user_id: str, endpoint: str, token: Optional[str],
                 per_page: int = 24, country: str = "NL", debug: bool = False):
    items = []
    # headers modeled after browser request (add required client-country header)
    headers = {
            "Accept"                    : "*/*",
            "Accept-Encoding"           : "gzip, deflate, br, zstd",
            "Accept-Language"           : "en",
            "User-Agent"                : session.headers.get("User-Agent", "mubi-export-script/1.0"),
            "client"                    : "web",
            "client-country"            : country,
            "anonymous_user_id"         : str(uuid.uuid4()),
            "Referer"                   : "https://mubi.com/",
            "Origin"                    : "https://mubi.com",
            "DNT"                       : "1",
            # optional helpful client hints (not required but mirror browser)
            "client-accept-audio-codecs": "aac",
            "client-accept-video-codecs": "h265,vp9,h264",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    params = {"per_page": per_page}
    url = BASE.format(user_id=user_id, endpoint=endpoint)
    before = None

    while True:
        if before is not None:
            params["before"] = str(before)
        try:
            data = get_json_with_retries(session, url, params, headers, debug=debug)
        except requests.HTTPError as he:
            raise RuntimeError(f"HTTP error while fetching {endpoint}: {he}") from he
        except Exception:
            raise

        # locate the list in the JSON (common keys: 'wishes', 'ratings', 'data')
        page_items = None
        if isinstance(data, dict):
            if "wishes" in data and isinstance(data["wishes"], list):
                page_items = data["wishes"]
            elif "ratings" in data and isinstance(data["ratings"], list):
                page_items = data["ratings"]
            elif "data" in data and isinstance(data["data"], list):
                page_items = data["data"]
            else:
                # recursive find first list
                def find_first_list(obj):
                    if isinstance(obj, list):
                        return obj
                    if isinstance(obj, dict):
                        for v in obj.values():
                            r = find_first_list(v)
                            if r is not None:
                                return r
                    return None

                page_items = find_first_list(data)
        elif isinstance(data, list):
            page_items = data
        else:
            page_items = []

        if not page_items:
            break

        items.extend(page_items)

        last = page_items[-1]
        cand_id = None
        if isinstance(last, dict):
            if "id" in last:
                cand_id = last["id"]
            elif "wish" in last and isinstance(last["wish"], dict) and "id" in last["wish"]:
                cand_id = last["wish"]["id"]
            elif "film" in last and isinstance(last["film"], dict) and "id" in last["film"]:
                cand_id = last["film"]["id"]
            else:
                def deep_find_id(o):
                    if isinstance(o, dict):
                        if "id" in o:
                            return o["id"]
                        for v in o.values():
                            r = deep_find_id(v)
                            if r is not None:
                                return r
                    if isinstance(o, list):
                        for e in o:
                            r = deep_find_id(e)
                            if r is not None:
                                return r
                    return None

                cand_id = deep_find_id(last)

        if cand_id is None:
            break

        try:
            before = int(cand_id)
        except Exception:
            before = cand_id

        time.sleep(0.05)

    return items


def extract_film_row_from_rating(rating_obj: dict):
    film = rating_obj.get("film") if isinstance(rating_obj, dict) else None
    row = {}
    row["rating_id"] = rating_obj.get("id")
    row["overall"] = rating_obj.get("overall") or rating_obj.get("rating") or rating_obj.get("score")
    row["created_at"] = rating_obj.get("created_at") or rating_obj.get("rated_at")
    if film and isinstance(film, dict):
        row["film_id"] = film.get("id")
        row["slug"] = film.get("slug")
        row["title"] = film.get("title")
        row["original_title"] = film.get("original_title")
        row["year"] = film.get("year")
        row["web_url"] = film.get("web_url")
        row["still_url"] = film.get("still_url") or (film.get("stills") or {}).get("standard")
    else:
        row.update({k: v for k, v in (rating_obj.items() if isinstance(rating_obj, dict) else [])})
    return row


def write_csv(path: str, rows: List[Dict[str, Any]]):
    if not rows:
        print(f"No rows to write for {path}")
        return
    keys = []
    for r in rows:
        for k in r.keys():
            if k not in keys:
                keys.append(k)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = _csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"Wrote {len(rows)} rows to {path}")

def clean_output(csv_path, letterboxd=False):
    is_ratings = 'ratings' in csv_path
    df = pd.read_csv(csv_path)

    # Base columns
    cols = ['film.id', 'film.title', 'film.original_title', 'film.year', 'film.duration', 'film.popularity',
            'film.genres', 'film.average_rating', 'film.average_rating_out_of_ten', 'film.number_of_ratings',
            'film.critic_review_rating', 'film.historic_countries', 'film.default_editorial',
            'film.directors']

    if is_ratings:
        cols += ['overall', 'created_at', 'body']
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce').dt.date

    df = df[cols]
    df.columns = [c.replace('film.', '') for c in df.columns]

    # Parse directors column
    df['directors'] = df['directors'].apply(literal_eval)
    df['directors'] = df['directors'].apply(lambda x: ', '.join([d.get('name', '') for d in
                                                                 x]) if isinstance(x, (list, tuple)) else x[
        'name'] if isinstance(x, dict) else '')

    # Rename rating columns for general CSV
    df = df.rename(columns={'overall': 'user_rating', 'created_at': 'rating_date', 'body': 'review'})

    if letterboxd:
        # Keep only Letterboxd-specific columns
        letterboxd_df = pd.DataFrame()
        letterboxd_df['Title'] = df['title']
        letterboxd_df['Year'] = df['year']
        letterboxd_df['Directors'] = df['directors']
        if is_ratings:
            letterboxd_df['Rating'] = df['user_rating']
            letterboxd_df['WatchedDate'] = df['rating_date']
            letterboxd_df['Review'] = df['review']

        # Save CSV with UTF-8
        letterboxd_path = csv_path.replace('.csv', '_letterboxd.csv')
        letterboxd_df.to_csv(letterboxd_path, index=False, encoding='utf-8')
    df.to_csv(csv_path, index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("user_id")
    parser.add_argument("--token", default=None, help="Bearer token (if required)")
    parser.add_argument("--per-page", type=int, default=24)
    parser.add_argument("--country", default="NL", help="client-country header (e.g. 'NL' or 'US')")
    parser.add_argument("--letterboxd", default=False, action="store_true", help="Also generate Letterboxd-compatible CSV")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update({"User-Agent": "mubi-export-script/1.0 (+https://github.com/)"})

    try:
        print("Fetching watchlist (wishes)...")
        wishes = paginate_api(session,
                              args.user_id,
                              "wishes",
                              args.token,
                              per_page=args.per_page,
                              country=args.country,
                              debug=args.debug)
        print(f"Got {len(wishes)} wishes.")
    except Exception as e:
        print("Failed to fetch watchlist:", e)
        print("If this persists, try: --debug and/or --token <your_bearer_token>")
        sys.exit(1)

    wish_rows = [flatten_json(w) for w in wishes]
    csv_path = f"mubi_{args.user_id}_watchlist.csv"
    write_csv(csv_path, wish_rows)
    clean_output(csv_path, letterboxd=args.letterboxd)

    print("Fetching ratings...")
    try:
        ratings = paginate_api(session,
                               args.user_id,
                               "ratings",
                               args.token,
                               per_page=args.per_page,
                               country=args.country,
                               debug=args.debug)
    except Exception as e:
        print("Failed to fetch ratings (will try fallbacks):", e)
        ratings = []
        for alt in ("marks", "reviews", "rated"):
            try:
                print(f"Trying fallback endpoint: {alt}")
                ratings = paginate_api(session,
                                       args.user_id,
                                       alt,
                                       args.token,
                                       per_page=args.per_page,
                                       country=args.country,
                                       debug=args.debug)
                if ratings:
                    print(f"Found results on endpoint: {alt}")
                    break
            except Exception as ee:
                debug_print(args.debug, f"Fallback {alt} error: {ee}")

    rating_rows = [flatten_json(r) for r in ratings]
    csv_path = f"mubi_{args.user_id}_ratings.csv"
    write_csv(csv_path, rating_rows)
    clean_output(csv_path, letterboxd=args.letterboxd)
    print("Done.")


if __name__ == "__main__":
    main()
