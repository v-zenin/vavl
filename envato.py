"""Collect Envato search-card video previews, then download them concurrently.

Input format (one search per line):
    Sahara Desert 50
    Cascade

The final whitespace-separated integer is the requested preview count.  When it
is absent, DEFAULT_COUNT is used.  Collection for every query finishes before
the five-worker download phase starts.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import mimetypes
import re
import tempfile
import traceback
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlparse

from playwright.async_api import APIRequestContext, Page, async_playwright


ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT_FILE = ROOT_DIR / "queries.txt"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "footages"
DEFAULT_COUNT = 30
DEFAULT_WORKERS = 5
ITEMS_PER_PAGE_ESTIMATE = 40
MAX_SEARCH_PAGES = 200
PROFILE_PATTERN = "puppeteer_dev_chrome_profile-*"
REQUEST_TIMEOUT = 1.0
SEARCH_URL_TEMPLATE = (
    "https://app.envato.com/search?itemType=stock-video&term={}"
    "&filter.orientation=Horizontal"
)


@dataclass(frozen=True)
class QueryTask:
    query: str
    count: int
    folder: str


@dataclass(frozen=True)
class CdpEndpoint:
    port: int

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


def find_whitetools_cdp() -> CdpEndpoint:
    profiles = sorted(
        Path(tempfile.gettempdir()).glob(PROFILE_PATTERN),
        key=lambda path: (path / "DevToolsActivePort").stat().st_mtime
        if (path / "DevToolsActivePort").exists() else 0,
        reverse=True,
    )
    for profile in profiles:
        try:
            port = int((profile / "DevToolsActivePort").read_text("utf-8").splitlines()[0])
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/json/version", timeout=REQUEST_TIMEOUT
            ) as response:
                json.load(response)
            return CdpEndpoint(port)
        except (OSError, ValueError, IndexError, json.JSONDecodeError,
                urllib.error.URLError):
            continue
    raise RuntimeError(
        "Active WhiteTools Chrome was not found. Start WhiteTools Chrome and try again."
    )


def safe_folder_name(query: str) -> str:
    value = re.sub(r"[^\w]+", "_", query.strip(), flags=re.UNICODE).strip("_.")
    return value or "query"


def load_queries(path: Path) -> list[QueryTask]:
    if not path.is_file():
        raise FileNotFoundError(f"Query file does not exist: {path}")
    tasks: list[QueryTask] = []
    used_folders: dict[str, int] = {}
    for line_number, original in enumerate(path.read_text("utf-8-sig").splitlines(), 1):
        line = original.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"(.+?)(?:\s+(\d+))?", line)
        if not match:
            raise ValueError(f"Invalid line {line_number}: {original!r}")
        query = match.group(1).strip()
        count = int(match.group(2)) if match.group(2) else DEFAULT_COUNT
        if not query or count < 1:
            raise ValueError(f"Invalid line {line_number}: query and count must be positive")
        base = safe_folder_name(query)
        occurrence = used_folders.get(base.casefold(), 0) + 1
        used_folders[base.casefold()] = occurrence
        folder = base if occurrence == 1 else f"{base}_{occurrence}"
        tasks.append(QueryTask(query, count, folder))
    if not tasks:
        raise ValueError(f"No queries found in {path}")
    return tasks


def decode_turbo_stream(text: str) -> Any:
    values = json.loads(text)
    cache: dict[int, Any] = {}

    def hydrate(reference: Any) -> Any:
        if not isinstance(reference, int) or isinstance(reference, bool):
            return reference
        if reference < 0:
            return None
        if reference in cache:
            return cache[reference]
        value = values[reference]
        if isinstance(value, list):
            result: list[Any] = []
            cache[reference] = result
            result.extend(hydrate(child) for child in value)
            return result
        if isinstance(value, dict):
            result_dict: dict[str, Any] = {}
            cache[reference] = result_dict
            for encoded_key, child in value.items():
                key = hydrate(int(encoded_key[1:])) if encoded_key.startswith("_") else encoded_key
                result_dict[str(key)] = hydrate(child)
            return result_dict
        cache[reference] = value
        return value

    return hydrate(0)


def find_video_items(value: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    seen: set[int] = set()

    def visit(node: Any) -> None:
        if isinstance(node, (dict, list)):
            marker = id(node)
            if marker in seen:
                return
            seen.add(marker)
        if isinstance(node, dict):
            if node.get("itemUuid") and node.get("itemType") == "stock-video":
                found.append(node)
                return
            for child in node.values():
                visit(child)
        elif isinstance(node, list):
            for child in node:
                visit(child)

    visit(value)
    return found


def is_watermarked_preview(url: str) -> bool:
    """Reject Envato preview assets whose filename explicitly marks a watermark."""
    return "watermarked_preview.mp4" in url.casefold()


async def collect_query(page: Page, task: QueryTask) -> list[dict[str, Any]]:
    search_url = SEARCH_URL_TEMPLATE.format(quote_plus(task.query))
    await page.goto(search_url, wait_until="domcontentloaded", timeout=60_000)
    endpoint = page.url.replace("/search?", "/search.data?", 1)
    query_string = page.url.split("?", 1)[1]
    collected: dict[str, dict[str, Any]] = {}
    filtered_watermarked = 0

    for page_number in range(1, MAX_SEARCH_PAGES + 1):
        form = (
            f"actionType=loadMore&{query_string}&page={page_number}"
            "&queryInterpretation%5Blocation%5D=true"
            "&features%5B%5D=enrollment_srp_pagination"
        )
        response = await page.request.post(
            endpoint,
            data=form,
            headers={"content-type": "application/x-www-form-urlencoded;charset=UTF-8"},
            timeout=30_000,
        )
        if not response.ok:
            raise RuntimeError(f"{task.query}: search API page {page_number}, HTTP {response.status}")
        decoded = decode_turbo_stream(await response.text())
        items = find_video_items(decoded)
        for item in items:
            item_id = str(item.get("itemUuid") or "")
            preview_url = str(item.get("videoUrl") or "")
            if item_id and preview_url and is_watermarked_preview(preview_url):
                filtered_watermarked += 1
                continue
            if item_id and preview_url and item_id not in collected:
                collected[item_id] = {
                    "item_id": item_id,
                    "title": str(item.get("title") or ""),
                    "preview_url": preview_url,
                    "item_url": f"https://app.envato.com/stock-video/{item_id}",
                    "query": task.query,
                    "folder": task.folder,
                }
                if len(collected) >= task.count:
                    break
        print(
            f"  {task.query}: page {page_number}, links {len(collected)}/{task.count}, "
            f"watermarked skipped {filtered_watermarked}"
        )
        if len(collected) >= task.count:
            break
        data = decoded.get("data", decoded) if isinstance(decoded, dict) else decoded
        if not items or (isinstance(data, dict) and data.get("hasNextPage") is False):
            break
    return list(collected.values())[:task.count]


def extension_for(url: str, content_type: str) -> str:
    extension = Path(urlparse(url).path).suffix.lower()
    if extension in {".mp4", ".webm", ".mov", ".m4v"}:
        return extension
    guessed = mimetypes.guess_extension(content_type.split(";", 1)[0].strip())
    return guessed if guessed in {".mp4", ".webm", ".mov", ".m4v"} else ".mp4"


async def download_one(
    request: APIRequestContext,
    item: dict[str, Any],
    number: int,
    output_dir: Path,
    force: bool,
) -> tuple[str, str]:
    folder = output_dir / item["folder"]
    folder.mkdir(parents=True, exist_ok=True)
    existing = next(
        (path for path in folder.glob(f"{number:03d}_{item['item_id']}.*")
         if not path.name.endswith(".part")),
        None,
    )
    if existing and existing.is_file() and existing.stat().st_size > 0 and not force:
        return "skipped", str(existing.resolve())

    response = await request.get(item["preview_url"], timeout=120_000)
    if not response.ok:
        raise RuntimeError(f"HTTP {response.status}")
    extension = extension_for(item["preview_url"], response.headers.get("content-type", ""))
    destination = folder / f"{number:03d}_{item['item_id']}{extension}"
    temporary = destination.with_suffix(destination.suffix + ".part")
    body = await response.body()
    await asyncio.to_thread(temporary.write_bytes, body)
    await response.dispose()
    temporary.replace(destination)
    return "downloaded", str(destination.resolve())


async def run(args: argparse.Namespace) -> tuple[int, int, int]:
    tasks = load_queries(args.input_file)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    endpoint = await asyncio.to_thread(find_whitetools_cdp)
    print(f"Queries: {len(tasks)}; default count: {DEFAULT_COUNT}")
    print("PHASE 1/2: collecting all preview links")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.connect_over_cdp(endpoint.url)
        if not browser.contexts:
            raise RuntimeError("WhiteTools Chrome has no browser context")
        context = browser.contexts[0]
        page = await context.new_page()
        all_items: list[dict[str, Any]] = []
        query_results: list[dict[str, Any]] = []
        try:
            for task in tasks:
                print(f"PARSE: {task.query} ({task.count})")
                items = await collect_query(page, task)
                for number, item in enumerate(items, 1):
                    item["number"] = number
                all_items.extend(items)
                query_results.append({
                    "query": task.query,
                    "requested": task.count,
                    "collected": len(items),
                    "folder": task.folder,
                    "items": items,
                })
                if len(items) < task.count:
                    print(f"  WARNING: only {len(items)} previews were available")

            links_file = args.output_dir / "collected_preview_links.json"
            links_file.write_text(json.dumps({
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "input_file": str(args.input_file.resolve()),
                "total": len(all_items),
                "queries": query_results,
            }, ensure_ascii=False, indent=2), "utf-8")
            print(f"Collected {len(all_items)} links. Manifest: {links_file.resolve()}")
            print(f"PHASE 2/2: downloading with {args.workers} workers")

            queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
            for item in all_items:
                queue.put_nowait(item)
            counters = {"downloaded": 0, "skipped": 0, "failed": 0}

            async def worker(worker_number: int) -> None:
                while True:
                    try:
                        item = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    label = f"{item['query']} #{item['number']}"
                    try:
                        status, path = await download_one(
                            context.request, item, item["number"], args.output_dir, args.force
                        )
                        counters[status] += 1
                        print(f"[worker {worker_number}] {status.upper()}: {label} -> {Path(path).name}")
                    except Exception as error:
                        counters["failed"] += 1
                        print(f"[worker {worker_number}] FAILED: {label}: {error}")
                    finally:
                        queue.task_done()

            worker_count = min(args.workers, len(all_items))
            await asyncio.gather(*(worker(i) for i in range(1, worker_count + 1)))
        finally:
            await page.close()

    return counters["downloaded"], counters["skipped"], counters["failed"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect Envato search preview URLs, then download 540p card videos."
    )
    parser.add_argument("--input-file", type=Path, default=DEFAULT_INPUT_FILE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, choices=range(1, 33))
    parser.add_argument("--force", action="store_true", help="download existing previews again")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        downloaded, skipped, failed = asyncio.run(run(args))
    except KeyboardInterrupt:
        print("Stopped. Run again to continue; completed files will be skipped.")
        raise SystemExit(130)
    except Exception as error:
        print(f"FATAL: {error}")
        traceback.print_exc()
        raise SystemExit(1)
    print(f"Done: downloaded={downloaded}, skipped={skipped}, failed={failed}")
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
