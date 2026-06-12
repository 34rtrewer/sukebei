#!/usr/bin/env python3
import asyncio
import json
import re
import random
import argparse
from pathlib import Path
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

# --- Configuration ---
H = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
}
BASE_URL   = "https://sukebei.nyaa.si/view/{}"
STATE_FILE = "crawler_state.json"
RESULTS_FILE = "nyaa_ec2_results.jsonl"

# --- Webshare 10个代理池 ---
PROXY_USER = "yygjrkml"
PROXY_PASS = "gly6hh1cnpkw"
PROXY_LIST = [
    ("38.154.203.95",   5863),
    ("198.105.121.200", 6462),
    ("64.137.96.74",    6641),
    ("209.127.138.10",  5784),
    ("38.154.185.97",   6370),
    ("84.247.60.125",   6095),
    ("142.111.67.146",  5611),
    ("191.96.254.138",  6185),
    ("31.58.9.4",       6077),
    ("104.239.107.47",  5699),
]

def proxy_url(index: int) -> str:
    ip, port = PROXY_LIST[index % len(PROXY_LIST)]
    return f"http://{PROXY_USER}:{PROXY_PASS}@{ip}:{port}"

def proxy_dict(index: int) -> dict:
    u = proxy_url(index)
    return {"http": u, "https": u}

# ============================================================
# HTML 解析
# ============================================================

def parse_html(html, id_val):
    soup = BeautifulSoup(html, "html.parser")
    if soup.find("div", class_="alert-danger"):
        return None
    title_tag = soup.find("h3", class_="panel-title")
    if not title_tag:
        return None

    res = {"id": id_val, "title": title_tag.get_text(strip=True)}

    magnet_tag = soup.find("a", href=re.compile(r"^magnet:\?"))
    res["magnet"] = magnet_tag["href"] if magnet_tag else None
    if res["magnet"]:
        h = re.search(r"btih:([a-fA-F0-9]{40})", res["magnet"])
        res["info_hash"] = h.group(1).lower() if h else None

    ts_tag = soup.find(attrs={"data-timestamp": True})
    if ts_tag:
        try:
            ts = int(ts_tag["data-timestamp"])
            res["uploaded_at"] = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except:
            pass

    def get_int(id_attr):
        tag = soup.find(id=id_attr)
        try:
            return int(tag.get_text(strip=True)) if tag else 0
        except:
            return 0

    res["seeders"]  = get_int("seeders")
    res["leechers"] = get_int("leechers")

    for row in soup.select(".panel-body .row"):
        cols = row.find_all("div", recursive=False)
        if len(cols) >= 2:
            key = cols[0].get_text(strip=True).rstrip(":")
            val = cols[1].get_text(strip=True)
            if key == "Category":        res["category"]    = val
            elif key == "Submitter":     res["submitter"]   = val
            elif key in ("File size", "Size"): res["size"]  = val
            elif key == "Completed":
                try:    res["completed"] = int(val)
                except: res["completed"] = 0
            elif key == "Information":
                a = cols[1].find("a")
                res["information"] = a["href"] if a else val

    desc = soup.find(id="torrent-description")
    res["description"] = desc.get_text(strip=True) if desc else None
    return res

# ============================================================
# 单次请求（每条任务传入自己的 proxies）
# ============================================================

async def fetch_one(id_val, proxies_for_this, min_delay, max_delay):
    await asyncio.sleep(random.uniform(min_delay, max_delay))
    try:
        async with AsyncSession(headers=H, proxies=proxies_for_this, impersonate="chrome110") as s:
            resp = await s.get(BASE_URL.format(id_val), timeout=15)
        if resp.status_code == 404:
            return id_val, None, "404"
        if resp.status_code == 429:
            return id_val, None, "429"
        resp.raise_for_status()
        data = parse_html(resp.text, id_val)
        return id_val, data, "ok" if data else "parse_fail"
    except Exception as e:
        return id_val, None, str(e)[:60]

# ============================================================
# 文件 & 状态
# ============================================================

def save_local_batch(results, fh):
    if not results:
        return
    for r in results:
        fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    fh.flush()

def save_state(progress, count):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({"progress": progress, "count": count}, f)
    except Exception as e:
        print(f" [!] Failed to save state: {e}")

def load_state():
    try:
        if Path(STATE_FILE).exists():
            with open(STATE_FILE) as f:
                return json.load(f)
    except:
        pass
    return {"progress": 4172147, "count": 0}

# ============================================================
# 主爬虫
# ============================================================

async def run_crawler(start_id, end_id, workers, min_delay, max_delay, proxy_arg, output_file, batch_size):
    state = load_state()
    if start_id == 4172147 and state["progress"] < 4172147:
        print(f"[*] Resuming from state: {state['progress']}")
        start_id = state["progress"]

    current_count = state.get("count", 0)

    # batch_size=0 表示跑完全部，否则限制条数
    if batch_size > 0:
        target_end = max(end_id, start_id - batch_size + 1)
    else:
        target_end = end_id

    if start_id < target_end:
        print("[*] Already reached end ID.")
        return

    use_pool = (proxy_arg is None)

    total = start_id - target_end + 1
    print(f"[*] {start_id} → {target_end}，共 {total} 条")
    print(f"[*] workers={workers}, delay={min_delay}-{max_delay}s")
    if use_pool:
        print(f"[*] 代理模式: 每条轮换（{len(PROXY_LIST)}个IP round-robin）")
    else:
        print(f"[*] 代理模式: 固定 {proxy_arg}")

    queue         = asyncio.Queue(maxsize=workers * 2)
    stop_event    = asyncio.Event()
    results_batch = []
    processed     = 0
    found         = 0
    global_seq    = 0   # 全局请求序号，用于 round-robin
    lock          = asyncio.Lock()

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = f"nyaa_{ts}.jsonl"
    fh = open(out_path, "w", encoding="utf-8")

    async def worker_task(thread_id: int):
        nonlocal processed, found, global_seq

        while not stop_event.is_set():
            try:
                id_val = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            # 每条任务拿一个序号 → round-robin 选代理
            async with lock:
                my_seq = global_seq
                global_seq += 1

            if use_pool:
                pd = proxy_dict(my_seq)         # round-robin
            else:
                pd = {"http": proxy_arg, "https": proxy_arg} if proxy_arg else None

            id_res, data, status = await fetch_one(id_val, pd, min_delay, max_delay)

            async with lock:
                processed += 1

                if data:
                    found += 1
                    results_batch.append(data)

                if status == "429":
                    cur_ip = PROXY_LIST[my_seq % len(PROXY_LIST)][0] if use_pool else proxy_arg
                    print(f" [!] 429 on #{id_val} via {cur_ip}, sleep 15s...")
                    await asyncio.sleep(15)

                if processed % 50 == 0:
                    if use_pool:
                        ip = PROXY_LIST[my_seq % len(PROXY_LIST)][0]
                        proxy_info = f"{ip} (slot {my_seq % len(PROXY_LIST)})"
                    else:
                        proxy_info = proxy_arg or "none"
                    print(f" [{processed}/{total}] ID:#{id_val} | found:{found} "
                          f"| {status} | {proxy_info}")

                if len(results_batch) >= 10 or processed % 50 == 0:
                    save_local_batch(results_batch, fh)
                    results_batch.clear()
                    save_state(id_val, current_count + found)

            queue.task_done()

    tasks = [asyncio.create_task(worker_task(i)) for i in range(workers)]

    for i in range(start_id, target_end - 1, -1):
        await queue.put(i)

    await queue.join()
    stop_event.set()

    if results_batch:
        save_local_batch(results_batch, fh)
    save_state(target_end - 1, current_count + found)

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    fh.close()
    print(f"\n[*] Done. processed={processed}, found={found} → {out_path}")

# ============================================================
# 入口
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start",      type=int,   default=4172147)
    parser.add_argument("--end",        type=int,   default=92)
    parser.add_argument("--workers",    type=int,   default=5)
    parser.add_argument("--proxy",      type=str,   default=None,
                        help="固定代理。不填则自动 round-robin 10个webshare代理")
    parser.add_argument("--min-delay",  type=float, default=0.8)
    parser.add_argument("--max-delay",  type=float, default=1.1)
    parser.add_argument("--output",     type=str,   default=RESULTS_FILE)
    parser.add_argument("--batch-size", type=int,   default=0,
                        help="0=跑完全部，>0=限制条数后退出")
    args = parser.parse_args()

    asyncio.run(run_crawler(
        args.start, args.end, args.workers,
        args.min_delay, args.max_delay,
        args.proxy, args.output, args.batch_size
    ))
