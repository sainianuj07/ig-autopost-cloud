#!/usr/bin/env python3
"""
Cloud publisher — runs in GitHub Actions on a cron, posts due items from
queue.json to Instagram + Facebook via the Meta Graph API. Media is already
hosted on the Shopify CDN (public URLs) by the local cloud_sync.py, so this
script only makes Graph API calls. State in state.json (committed back by the
workflow after each run).

queue.json format:
  [{"key": "shribazaar/2026-08-26/p1", "ig_username": "shribazaar",
    "type": "post|carousel|story", "time": "2026-08-26 09:00",
    "media": ["https://.../igv-....mp4"], "caption": "...", "fb": true}, ...]

Env: META_PUBLISH_TOKEN (repo secret). Times are IST.
"""
import json, os, sys, time, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))
GRAPH = "https://graph.facebook.com/v21.0"
HERE = os.path.dirname(os.path.abspath(__file__))
QUEUE = os.path.join(HERE, "queue.json")
STATE = os.path.join(HERE, "state.json")
MAX_LATE_HOURS = 6
# Burst guard: when the GitHub schedule stalls for hours, a catch-up run must not
# dump the whole backlog back-to-back (IG spam standard punishes bursty repetition).
MAX_PER_RUN = 6
MIN_GAP_SEC = 150
TOKEN = os.environ.get("META_PUBLISH_TOKEN", "").strip()


def log(msg):
    print(f"[{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def api(path, params=None, post=False, tok=None, retries=3):
    params = dict(params or {})
    params["access_token"] = tok or TOKEN
    data = urllib.parse.urlencode(params).encode()
    url = f"{GRAPH}/{path}"
    if not post:
        url += "?" + data.decode()
        data = None
    last = None
    for i in range(retries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=120) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:500]
            last = f"HTTP {e.code}: {body}"
            transient = e.code >= 500 or '"code":4' in body or '"code":17' in body or '"code":32' in body
            if not transient:
                raise RuntimeError(last)
        except Exception as e:
            last = str(e)
        time.sleep(20 * (i + 1))
    raise RuntimeError(f"giving up: {last}")


def resolve_accounts():
    res = api("me/accounts", {"fields": "id,name,access_token,instagram_business_account{id,username}", "limit": "100"})
    out = {}
    for page in res.get("data", []):
        ig = page.get("instagram_business_account") or {}
        if ig.get("username"):
            out[ig["username"].lower()] = {"ig_user_id": ig["id"], "page_id": page["id"],
                                           "page_token": page["access_token"]}
    return out


def wait_container(cid, tok, tries=24):
    for _ in range(tries):
        st = api(f"{cid}", {"fields": "status_code"}, tok=tok)
        if st.get("status_code") == "FINISHED":
            return
        if st.get("status_code") == "ERROR":
            raise RuntimeError(f"container {cid} errored: {st}")
        time.sleep(5)
    raise RuntimeError(f"container {cid} never finished")


def publish_instagram(acct, item, urls):
    ig, tok = acct["ig_user_id"], acct["page_token"]
    video = urls[0].endswith(".mp4")
    if item["type"] == "story" and video:
        c = api(f"{ig}/media", {"media_type": "STORIES", "video_url": urls[0]}, post=True, tok=tok)
        wait_container(c["id"], tok, tries=60)
        return api(f"{ig}/media_publish", {"creation_id": c["id"]}, post=True, tok=tok).get("id")
    if item["type"] == "post" and video:
        c = api(f"{ig}/media", {"media_type": "REELS", "video_url": urls[0],
                                "caption": item.get("caption", ""), "share_to_feed": "true"}, post=True, tok=tok)
        wait_container(c["id"], tok, tries=60)
        return api(f"{ig}/media_publish", {"creation_id": c["id"]}, post=True, tok=tok).get("id")
    if item["type"] == "story":
        c = api(f"{ig}/media", {"media_type": "STORIES", "image_url": urls[0]}, post=True, tok=tok)
    elif item["type"] == "carousel":
        children = []
        for u in urls:
            r = api(f"{ig}/media", {"image_url": u, "is_carousel_item": "true"}, post=True, tok=tok)
            wait_container(r["id"], tok)
            children.append(r["id"])
        c = api(f"{ig}/media", {"media_type": "CAROUSEL", "children": ",".join(children),
                                "caption": item.get("caption", "")}, post=True, tok=tok)
    else:
        c = api(f"{ig}/media", {"image_url": urls[0], "caption": item.get("caption", "")}, post=True, tok=tok)
    wait_container(c["id"], tok)
    return api(f"{ig}/media_publish", {"creation_id": c["id"]}, post=True, tok=tok).get("id")


def publish_facebook(acct, item, urls):
    pid, tok = acct["page_id"], acct["page_token"]
    video = urls[0].endswith(".mp4")
    if item["type"] == "post" and video:
        return api(f"{pid}/videos", {"file_url": urls[0], "description": item.get("caption", "")},
                   post=True, tok=tok).get("id")
    if item["type"] == "story" and video:
        start = api(f"{pid}/video_stories", {"upload_phase": "start"}, post=True, tok=tok)
        vid, up_url = start["video_id"], start["upload_url"]
        req = urllib.request.Request(up_url, data=b"", method="POST",
                                     headers={"Authorization": f"OAuth {tok}", "file_url": urls[0]})
        with urllib.request.urlopen(req, timeout=300) as resp:
            json.loads(resp.read())
        for _ in range(30):
            st = api(f"{vid}", {"fields": "status"}, tok=tok)
            if (st.get("status") or {}).get("uploading_phase", {}).get("status") == "complete":
                break
            time.sleep(5)
        r = api(f"{pid}/video_stories", {"video_id": vid, "upload_phase": "finish"}, post=True, tok=tok)
        return r.get("post_id") or vid
    if item["type"] == "story":
        ph = api(f"{pid}/photos", {"url": urls[0], "published": "false"}, post=True, tok=tok)
        r = api(f"{pid}/photo_stories", {"photo_id": ph["id"]}, post=True, tok=tok)
        return r.get("post_id") or r.get("id")
    if item["type"] == "carousel":
        media = []
        for u in urls:
            ph = api(f"{pid}/photos", {"url": u, "published": "false"}, post=True, tok=tok)
            media.append({"media_fbid": ph["id"]})
        return api(f"{pid}/feed", {"message": item.get("caption", ""),
                                   "attached_media": json.dumps(media)}, post=True, tok=tok).get("id")
    r = api(f"{pid}/photos", {"url": urls[0], "message": item.get("caption", "")}, post=True, tok=tok)
    return r.get("post_id") or r.get("id")


def main():
    if not TOKEN:
        sys.exit("META_PUBLISH_TOKEN not set")
    queue = json.load(open(QUEUE)) if os.path.exists(QUEUE) else []
    state = json.load(open(STATE)) if os.path.exists(STATE) else {}
    now = datetime.now(IST)
    horizon = now - timedelta(hours=MAX_LATE_HOURS)
    due = []
    for item in queue:
        key = item["key"]
        if key in state:
            continue
        t = datetime.strptime(item["time"], "%Y-%m-%d %H:%M").replace(tzinfo=IST)
        if t > now:
            continue
        if t < horizon:
            state[key] = {"status": "skipped_too_late", "at": now.isoformat()}
            log(f"SKIP (too late): {key}")
            continue
        due.append(item)
    if not due:
        json.dump(state, open(STATE, "w"), indent=1)
        log("nothing due")
        return
    accounts = resolve_accounts()
    due = sorted(due, key=lambda i: i["time"])
    if len(due) > MAX_PER_RUN:
        log(f"pacing: {len(due)} due, posting {MAX_PER_RUN} this run, "
            f"{len(due) - MAX_PER_RUN} deferred to next run")
        due = due[:MAX_PER_RUN]
    posted = 0
    for item in due:
        key, want = item["key"], item["ig_username"].lower().lstrip("@")
        if posted:
            time.sleep(MIN_GAP_SEC)
        if want not in accounts:
            log(f"HOLD {key}: no page with IG @{want} on this token")
            continue
        try:
            ig_id = publish_instagram(accounts[want], item, item["media"])
            fb_id = publish_facebook(accounts[want], item, item["media"]) if item.get("fb", True) else None
            state[key] = {"status": "published", "ig_id": ig_id, "fb_id": fb_id,
                          "at": datetime.now(IST).isoformat(), "by": "cloud"}
            log(f"OK {key}: ig={ig_id} fb={fb_id}")
            posted += 1
        except Exception as e:
            tries = state.get(key + "__tries", 0)
            if tries >= 2:
                state[key] = {"status": "failed", "error": str(e)[:300], "at": datetime.now(IST).isoformat()}
                log(f"FAIL (final) {key}: {e}")
            else:
                state[key + "__tries"] = tries + 1
                log(f"FAIL (retry {tries+1}/3 next run) {key}: {e}")
        json.dump(state, open(STATE, "w"), indent=1)


if __name__ == "__main__":
    main()
