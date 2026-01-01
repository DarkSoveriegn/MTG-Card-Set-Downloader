import asyncio
import threading
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import time
import json
from datetime import datetime

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import aiohttp
import mysql.connector

from PIL import Image, ImageTk

# -----------------------------
# Config / constants
# -----------------------------
SCRYFALL_API = "https://api.scryfall.com/cards/search?q=e%3A{}"
LOG_FILE = "mtg_downloader.log"
CONFIG_FILE = "config.json"

# -----------------------------
# Logging setup
# -----------------------------
logger = logging.getLogger("mtg_downloader")
logger.setLevel(logging.INFO)

file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)


class TkinterLogHandler(logging.Handler):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        self.text_widget.after(0, self._append, msg)

    def _append(self, msg):
        self.text_widget.insert(tk.END, msg + "\n")
        self.text_widget.see(tk.END)


# -----------------------------
# Utility functions
# -----------------------------
def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def load_set_codes(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def load_config():
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_config(cfg):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=4)
    except Exception as e:
        logger.error(f"Failed to save config: {e}")


# -----------------------------
# MySQL helpers
# -----------------------------
def create_mysql_connection(host, user, password, database):
    if not host or not user or not database:
        return None

    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to MySQL: {e}")
        return None


def ensure_schema(conn):
    if conn is None:
        return
    cursor = conn.cursor()

    create_sets = """
    CREATE TABLE IF NOT EXISTS sets (
        id INT AUTO_INCREMENT PRIMARY KEY,
        set_code VARCHAR(16) NOT NULL UNIQUE,
        set_name VARCHAR(255),
        total_cards INT,
        last_checked DATETIME,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    create_cards = """
    CREATE TABLE IF NOT EXISTS cards (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(64) NOT NULL UNIQUE,
        set_code VARCHAR(16) NOT NULL,
        name VARCHAR(255),
        rarity VARCHAR(32),
        collector_number VARCHAR(32),
        json_data JSON,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (set_code) REFERENCES sets(set_code)
            ON UPDATE CASCADE ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    create_card_images = """
    CREATE TABLE IF NOT EXISTS card_images (
        id INT AUTO_INCREMENT PRIMARY KEY,
        card_id VARCHAR(64) NOT NULL,
        set_code VARCHAR(16) NOT NULL,
        image_path TEXT NOT NULL,
        downloaded_at DATETIME NOT NULL,
        UNIQUE KEY unique_card_set (card_id, set_code),
        FOREIGN KEY (card_id) REFERENCES cards(card_id)
            ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY (set_code) REFERENCES sets(set_code)
            ON UPDATE CASCADE ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    create_config = """
    CREATE TABLE IF NOT EXISTS config (
        config_key VARCHAR(128) PRIMARY KEY,
        config_value TEXT NOT NULL,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    cursor.execute(create_sets)
    cursor.execute(create_cards)
    cursor.execute(create_card_images)
    cursor.execute(create_config)
    conn.commit()
    cursor.close()


def upsert_set_metadata(conn, set_code, set_name, total_cards):
    if conn is None:
        return
    try:
        cursor = conn.cursor()
        sql = """
        INSERT INTO sets (set_code, set_name, total_cards, last_checked)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            set_name = VALUES(set_name),
            total_cards = VALUES(total_cards),
            last_checked = VALUES(last_checked)
        """
        cursor.execute(sql, (set_code, set_name, total_cards, datetime.utcnow()))
        conn.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Failed to upsert set metadata for {set_code}: {e}")


def upsert_card_metadata(conn, card, set_code):
    if conn is None:
        return
    try:
        card_id = card.get("id")
        name = card.get("name")
        rarity = card.get("rarity")
        collector_number = card.get("collector_number")
        json_data = json.dumps(card)

        cursor = conn.cursor()
        sql = """
        INSERT INTO cards (card_id, set_code, name, rarity, collector_number, json_data)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            set_code = VALUES(set_code),
            name = VALUES(name),
            rarity = VALUES(rarity),
            collector_number = VALUES(collector_number),
            json_data = VALUES(json_data)
        """
        cursor.execute(sql, (card_id, set_code, name, rarity, collector_number, json_data))
        conn.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Failed to upsert card metadata for {set_code}/{card.get('id')}: {e}")


def insert_image_metadata(conn, card_id, set_code, image_path):
    if conn is None:
        return
    try:
        cursor = conn.cursor()
        sql = """
        INSERT INTO card_images (card_id, set_code, image_path, downloaded_at)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE image_path=VALUES(image_path), downloaded_at=VALUES(downloaded_at)
        """
        cursor.execute(sql, (card_id, set_code, str(image_path), datetime.utcnow()))
        conn.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Failed to insert image metadata for {card_id} ({set_code}): {e}")


def get_set_expected_card_count(conn, set_code):
    if conn is None:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT total_cards FROM sets WHERE set_code=%s", (set_code,))
        row = cursor.fetchone()
        cursor.close()
        if row:
            return row[0]
        return None
    except Exception as e:
        logger.error(f"Failed to get total_cards for set {set_code}: {e}")
        return None


def is_set_complete(conn, set_code, root_dir: Path):
    set_dir = root_dir / set_code
    if not set_dir.exists() or not set_dir.is_dir():
        return False
    files = [f for f in set_dir.iterdir() if f.is_file()]
    if not files:
        return False

    expected = get_set_expected_card_count(conn, set_code)
    if expected is None:
        return len(files) > 0
    return len(files) >= expected


def get_card_info(conn, card_id):
    """Return dict with name, set_code, rarity, collector_number, or None."""
    if conn is None:
        return None
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT name, set_code, rarity, collector_number FROM cards WHERE card_id=%s",
            (card_id,),
        )
        row = cursor.fetchone()
        cursor.close()
        return row
    except Exception as e:
        logger.error(f"Failed to get card info for {card_id}: {e}")
        return None


# -----------------------------
# Async HTTP helpers
# -----------------------------
async def fetch_json(session, url, retries=5, stop_cb=None):
    delay = 2
    for attempt in range(1, retries + 1):
        if stop_cb and stop_cb():
            raise asyncio.CancelledError("Stop requested during JSON fetch")

        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.warning(f"Non-200 response {resp.status} for {url}")
        except Exception as e:
            logger.error(f"Error fetching JSON: {e}")

        logger.info(f"Retrying JSON fetch in {delay} seconds... (attempt {attempt}/{retries})")
        await asyncio.sleep(delay)
        delay *= 1.5

    raise RuntimeError(f"Failed to fetch JSON after {retries} retries: {url}")


async def download_image(session, url, dest: Path, retries=5, stop_cb=None):
    delay = 2
    for attempt in range(1, retries + 1):
        if stop_cb and stop_cb():
            return False

        try:
            await asyncio.sleep(2)  # delay before each file start

            if stop_cb and stop_cb():
                return False

            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    dest.write_bytes(data)
                    return True
                else:
                    logger.warning(f"Image HTTP {resp.status} for {url}")

        except Exception as e:
            logger.error(f"Error downloading image: {e}")

        if stop_cb and stop_cb():
            return False

        logger.info(f"Retrying image in {delay} seconds... (attempt {attempt}/{retries})")
        await asyncio.sleep(delay)
        delay *= 1.5

    logger.error(f"Failed to download image after {retries} retries: {url}")
    return False


# -----------------------------
# Download logic
# -----------------------------
async def download_set(session, set_code: str, output_root: Path, conn, gui_callbacks, concurrency: int):
    stop_cb = gui_callbacks["stop_requested"]

    if stop_cb():
        return

    logger.info(f"Processing set: {set_code}")
    gui_callbacks["log_info"](f"Processing set: {set_code}")

    set_dir = output_root / set_code
    ensure_dir(set_dir)

    url = SCRYFALL_API.format(set_code)
    cards = []

    while url:
        data = await fetch_json(session, url, stop_cb=stop_cb)
        cards.extend(data.get("data", []))
        url = data.get("next_page")

    total_cards = len(cards)
    set_name = cards[0].get("set_name") if cards else None
    upsert_set_metadata(conn, set_code, set_name, total_cards)

    logger.info(f"Found {total_cards} cards in set {set_code}")
    gui_callbacks["log_info"](f"Found {total_cards} cards in set {set_code}")
    gui_callbacks["reset_set_progress"](total_cards)

    sem = asyncio.Semaphore(max(concurrency, 1))
    completed = 0
    completed_lock = asyncio.Lock()

    async def process_card(card):
        nonlocal completed
        if stop_cb():
            return

        upsert_card_metadata(conn, card, set_code)

        if "image_uris" not in card:
            async with completed_lock:
                completed += 1
            gui_callbacks["increment_set_progress"]()
            return

        img_url = card["image_uris"].get("large") or card["image_uris"].get("normal")
        if not img_url:
            async with completed_lock:
                completed += 1
            gui_callbacks["increment_set_progress"]()
            return

        card_id = card["id"]
        file_name = f"{card_id}.jpg"
        dest = set_dir / file_name

        if dest.exists():
            logger.info(f"Image already exists, skipping download: {dest}")
            gui_callbacks["log_info"](f"Existing image, skipping: {dest.name}")
            insert_image_metadata(conn, card_id, set_code, dest)
            async with completed_lock:
                completed += 1
            gui_callbacks["increment_set_progress"]()
            return

        async with sem:
            if stop_cb():
                async with completed_lock:
                    completed += 1
                gui_callbacks["increment_set_progress"]()
                return
            ok = await download_image(session, img_url, dest, stop_cb=stop_cb)

        if ok:
            logger.info(f"Downloaded {dest}")
            gui_callbacks["log_info"](f"Downloaded {dest.name}")
            insert_image_metadata(conn, card_id, set_code, dest)

        async with completed_lock:
            completed += 1
        gui_callbacks["increment_set_progress"]()

    tasks = [asyncio.create_task(process_card(card)) for card in cards]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    logger.info(f"Completed set: {set_code}")
    gui_callbacks["log_info"](f"Completed set: {set_code}")


async def download_all_sets_async(set_file, root_dir, mysql_cfg, gui_callbacks, concurrency: int, skip_completed: bool):
    stop_cb = gui_callbacks["stop_requested"]

    set_codes = load_set_codes(set_file)
    total_sets = len(set_codes)

    logger.info(f"Loaded {total_sets} set codes from {set_file}")
    gui_callbacks["log_info"](f"Loaded {total_sets} set codes")

    output_root = Path(root_dir)
    ensure_dir(output_root)

    conn = create_mysql_connection(
        mysql_cfg["host"],
        mysql_cfg["user"],
        mysql_cfg["password"],
        mysql_cfg["database"],
    )
    if conn:
        ensure_schema(conn)
        gui_callbacks["log_info"]("Connected to MySQL and ensured schema.")
    else:
        gui_callbacks["log_info"]("MySQL disabled or connection failed; continuing without DB cache.")

    async with aiohttp.ClientSession() as session:
        gui_callbacks["reset_overall_progress"](total_sets)
        completed_sets = 0

        for code in set_codes:
            if stop_cb():
                break

            if skip_completed and is_set_complete(conn, code, output_root):
                logger.info(f"Skipping already completed set: {code}")
                gui_callbacks["log_info"](f"Skipping completed set: {code}")
                completed_sets += 1
                gui_callbacks["update_overall_progress"](completed_sets)
                continue

            gui_callbacks["set_current_set_label"](code)
            await download_set(session, code, output_root, conn, gui_callbacks, concurrency)
            completed_sets += 1
            gui_callbacks["update_overall_progress"](completed_sets)
            if stop_cb():
                break
            await asyncio.sleep(2)

    if conn:
        conn.close()
        gui_callbacks["log_info"]("Closed MySQL connection.")

    if stop_cb():
        gui_callbacks["log_info"]("Download stopped by user.")
    else:
        gui_callbacks["log_info"]("All sets completed.")

    gui_callbacks["on_all_done"]()


async def verify_or_fix_all_sets_async(root_dir, mysql_cfg, gui_callbacks, concurrency: int, fix_missing: bool):
    """
    If fix_missing is False: verify only, log missing.
    If fix_missing is True: re-download missing/corrupt images.
    """
    stop_cb = gui_callbacks["stop_requested"]

    root = Path(root_dir)
    if not root.exists():
        gui_callbacks["log_info"]("Root directory does not exist for verification.")
        gui_callbacks["on_all_done"]()
        return

    conn = create_mysql_connection(
        mysql_cfg["host"],
        mysql_cfg["user"],
        mysql_cfg["password"],
        mysql_cfg["database"],
    )
    if conn:
        ensure_schema(conn)
        gui_callbacks["log_info"]("Connected to MySQL for verification.")
    else:
        gui_callbacks["log_info"]("MySQL disabled or connection failed; verification will only use filesystem.")

    set_dirs = [p for p in root.iterdir() if p.is_dir()]
    total_sets = len(set_dirs)
    gui_callbacks["log_info"](f"Verifying {total_sets} set folders under root.")
    gui_callbacks["reset_overall_progress"](total_sets)

    async with aiohttp.ClientSession() as session:
        completed_sets = 0
        for set_dir in set_dirs:
            if stop_cb():
                break

            set_code = set_dir.name
            action_label = "Fix" if fix_missing else "Verify"
            gui_callbacks["set_current_set_label"](f"{action_label}: {set_code}")
            logger.info(f"{action_label} set {set_code}")
            gui_callbacks["log_info"](f"{action_label} set {set_code}")

            url = SCRYFALL_API.format(set_code)
            cards = []
            while url:
                data = await fetch_json(session, url, stop_cb=stop_cb)
                cards.extend(data.get("data", []))
                url = data.get("next_page")

            total_cards = len(cards)
            gui_callbacks["reset_set_progress"](total_cards)

            missing = 0
            sem = asyncio.Semaphore(max(concurrency, 1))
            completed = 0
            completed_lock = asyncio.Lock()

            async def process_card(card):
                nonlocal missing, completed
                if stop_cb():
                    return

                if "id" not in card:
                    async with completed_lock:
                        completed += 1
                    gui_callbacks["increment_set_progress"]()
                    return

                card_id = card["id"]
                dest = set_dir / f"{card_id}.jpg"

                # Check existence
                async with sem:
                    exists = dest.exists() and dest.is_file() and dest.stat().st_size > 0

                    if not exists:
                        missing += 1
                        msg = f"Missing or empty image: {set_code}/{dest.name}"
                        logger.warning(msg)
                        gui_callbacks["log_info"](msg)

                        if fix_missing:
                            # ensure card metadata
                            upsert_card_metadata(conn, card, set_code)
                            img_url = card.get("image_uris", {}).get("large") or card.get("image_uris", {}).get("normal")
                            if img_url:
                                ok = await download_image(session, img_url, dest, stop_cb=stop_cb)
                                if ok:
                                    insert_image_metadata(conn, card_id, set_code, dest)
                                    logger.info(f"Re-downloaded missing image: {dest}")
                                    gui_callbacks["log_info"](f"Re-downloaded: {set_code}/{dest.name}")

                async with completed_lock:
                    completed += 1
                gui_callbacks["increment_set_progress"]()

            tasks = [asyncio.create_task(process_card(card)) for card in cards]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            if not fix_missing:
                if missing == 0:
                    gui_callbacks["log_info"](f"Set {set_code}: all images present.")
                else:
                    gui_callbacks["log_info"](f"Set {set_code}: {missing} images missing or invalid.")
            else:
                if missing == 0:
                    gui_callbacks["log_info"](f"Set {set_code}: nothing missing to fix.")
                else:
                    gui_callbacks["log_info"](f"Set {set_code}: attempted to fix {missing} missing/invalid images.")

            completed_sets += 1
            gui_callbacks["update_overall_progress"](completed_sets)
            if stop_cb():
                break
            await asyncio.sleep(1)

    if conn:
        conn.close()
        gui_callbacks["log_info"]("Closed MySQL connection after verification/fix.")

    if stop_cb():
        gui_callbacks["log_info"]("Verification/re-download stopped by user.")
    else:
        if fix_missing:
            gui_callbacks["log_info"]("Re-download of missing images completed.")
        else:
            gui_callbacks["log_info"]("Verification completed.")

    gui_callbacks["on_all_done"]()


# -----------------------------
# GUI application
# -----------------------------
class MTGDownloaderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("MTG Set Image Downloader")

        cfg = load_config()

        self.set_file_path = tk.StringVar(value=cfg.get("last_set_file", ""))
        self.root_storage_dir = tk.StringVar(value=cfg.get("root_directory", ""))

        mysql_cfg = cfg.get("mysql", {})
        self.mysql_host = tk.StringVar(value=mysql_cfg.get("host", "localhost"))
        self.mysql_user = tk.StringVar(value=mysql_cfg.get("user", ""))
        self.mysql_password = tk.StringVar(value=mysql_cfg.get("password", ""))
        self.mysql_database = tk.StringVar(value=mysql_cfg.get("database", ""))

        self.concurrency = tk.IntVar(value=cfg.get("concurrency", 4))
        self.resume_mode = tk.BooleanVar(value=cfg.get("resume_mode", True))

        self.is_running = False
        self.stop_requested = False

        self.preview_image = None  # keep reference to avoid GC

        # Card info panel state
        self.card_name_var = tk.StringVar(value="-")
        self.card_set_var = tk.StringVar(value="-")
        self.card_rarity_var = tk.StringVar(value="-")
        self.card_collector_var = tk.StringVar(value="-")

        self._build_ui()
        self._configure_logging_bridge()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_ui(self):
        main = ttk.Frame(self.root, padding=10)
        main.pack(fill=tk.BOTH, expand=True)

        # Set list file
        file_frame = ttk.LabelFrame(main, text="Set list file")
        file_frame.pack(fill=tk.X, pady=5)

        ttk.Entry(file_frame, textvariable=self.set_file_path, width=60).pack(
            side=tk.LEFT, padx=5, pady=5, fill=tk.X, expand=True
        )
        ttk.Button(file_frame, text="Browse...", command=self.browse_set_file).pack(
            side=tk.LEFT, padx=5, pady=5
        )

        # Root storage directory
        root_frame = ttk.LabelFrame(main, text="Root storage directory")
        root_frame.pack(fill=tk.X, pady=5)

        ttk.Entry(root_frame, textvariable=self.root_storage_dir, width=60).pack(
            side=tk.LEFT, padx=5, pady=5, fill=tk.X, expand=True
        )
        ttk.Button(root_frame, text="Browse...", command=self.browse_root_dir).pack(
            side=tk.LEFT, padx=5, pady=5
        )

        # MySQL config
        db_frame = ttk.LabelFrame(main, text="MySQL metadata cache")
        db_frame.pack(fill=tk.X, pady=5)

        self._add_labeled_entry(db_frame, "Host:", self.mysql_host)
        self._add_labeled_entry(db_frame, "User:", self.mysql_user)
        self._add_labeled_entry(db_frame, "Password:", self.mysql_password, show="*")
        self._add_labeled_entry(db_frame, "Database:", self.mysql_database)

        # Options
        opt_frame = ttk.LabelFrame(main, text="Options")
        opt_frame.pack(fill=tk.X, pady=5)

        # Concurrency slider
        c_frame = ttk.Frame(opt_frame)
        c_frame.pack(fill=tk.X, padx=5, pady=2)
        ttk.Label(c_frame, text="Concurrency (images per set):", width=25).pack(side=tk.LEFT)
        ttk.Scale(
            c_frame,
            from_=1,
            to=16,
            orient=tk.HORIZONTAL,
            variable=self.concurrency,
        ).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Label(c_frame, textvariable=self.concurrency, width=4).pack(side=tk.LEFT)

        # Resume mode checkbox
        ttk.Checkbutton(
            opt_frame,
            text="Skip sets that appear already complete (resume mode)",
            variable=self.resume_mode,
        ).pack(anchor="w", padx=5, pady=2)

        # Progress section
        prog_frame = ttk.LabelFrame(main, text="Progress")
        prog_frame.pack(fill=tk.X, pady=5)

        self.current_set_label = ttk.Label(prog_frame, text="Current action: -")
        self.current_set_label.pack(anchor="w", padx=5, pady=2)

        ttk.Label(prog_frame, text="Overall sets:").pack(anchor="w", padx=5)
        self.overall_progress = ttk.Progressbar(prog_frame, maximum=100)
        self.overall_progress.pack(fill=tk.X, padx=5, pady=2)

        ttk.Label(prog_frame, text="Current set cards:").pack(anchor="w", padx=5)
        self.set_progress = ttk.Progressbar(prog_frame, maximum=100)
        self.set_progress.pack(fill=tk.X, padx=5, pady=2)

        # Start/Stop/Verify buttons
        btn_frame = ttk.Frame(main)
        btn_frame.pack(fill=tk.X, pady=5)

        self.start_button = ttk.Button(btn_frame, text="Start Download", command=self.start_download)
        self.start_button.pack(side=tk.LEFT, padx=5)

        self.verify_button = ttk.Button(btn_frame, text="Verify Images", command=self.start_verify_only)
        self.verify_button.pack(side=tk.LEFT, padx=5)

        self.fix_missing_button = ttk.Button(btn_frame, text="Re-download missing only", command=self.start_verify_and_fix)
        self.fix_missing_button.pack(side=tk.LEFT, padx=5)

        self.stop_button = ttk.Button(btn_frame, text="Stop", command=self.stop_download, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5)

        # Split: left tree + right log+preview+info
        bottom_frame = ttk.PanedWindow(main, orient=tk.HORIZONTAL)
        bottom_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        # Left: directory tree
        tree_frame = ttk.LabelFrame(bottom_frame, text="Directory tree")
        bottom_frame.add(tree_frame, weight=1)

        self.tree = ttk.Treeview(tree_frame, columns=("type",), show="tree")
        self.tree.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)

        tree_scroll = ttk.Scrollbar(tree_frame, command=self.tree.yview)
        tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.configure(yscrollcommand=tree_scroll.set)

        self.tree.bind("<<TreeviewSelect>>", self.on_tree_select)

        refresh_tree_btn = ttk.Button(tree_frame, text="Refresh tree", command=self.refresh_tree)
        refresh_tree_btn.pack(fill=tk.X, padx=5, pady=2)

        # Right: log + preview + card info
        right_frame = ttk.PanedWindow(bottom_frame, orient=tk.VERTICAL)
        bottom_frame.add(right_frame, weight=2)

        # Log viewer
        log_frame = ttk.LabelFrame(right_frame, text="Log")
        right_frame.add(log_frame, weight=2)

        self.log_text = tk.Text(log_frame, wrap="word", height=10)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text["yscrollcommand"] = scroll.set

        # Preview & card info container
        lower_right = ttk.PanedWindow(right_frame, orient=tk.VERTICAL)
        right_frame.add(lower_right, weight=2)

        # Preview pane
        preview_frame = ttk.LabelFrame(lower_right, text="Card preview")
        lower_right.add(preview_frame, weight=3)

        self.preview_label = ttk.Label(preview_frame, text="Select an image file in the tree.")
        self.preview_label.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Card info pane
        info_frame = ttk.LabelFrame(lower_right, text="Card info")
        lower_right.add(info_frame, weight=1)

        self._add_info_row(info_frame, "Name:", self.card_name_var)
        self._add_info_row(info_frame, "Set:", self.card_set_var)
        self._add_info_row(info_frame, "Rarity:", self.card_rarity_var)
        self._add_info_row(info_frame, "Collector #:", self.card_collector_var)

        # Styling
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("Horizontal.TProgressbar", troughcolor="#333333", background="#4caf50")

    def _add_labeled_entry(self, parent, label_text, var, show=None):
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.X, padx=5, pady=2)
        ttk.Label(frame, text=label_text, width=10).pack(side=tk.LEFT)
        ttk.Entry(frame, textvariable=var, show=show).pack(side=tk.LEFT, fill=tk.X, expand=True)

    def _add_info_row(self, parent, label_text, var):
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.X, padx=5, pady=2)
        ttk.Label(frame, text=label_text, width=12).pack(side=tk.LEFT)
        ttk.Label(frame, textvariable=var).pack(side=tk.LEFT, fill=tk.X, expand=True)

    def _configure_logging_bridge(self):
        gui_handler = TkinterLogHandler(self.log_text)
        gui_handler.setFormatter(file_formatter)
        logger.addHandler(gui_handler)

    # ------------------ GUI actions ------------------ #
    def browse_set_file(self):
        path = filedialog.askopenfilename(
            title="Select set list file",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if path:
            self.set_file_path.set(path)
            self.refresh_tree()

    def browse_root_dir(self):
        path = filedialog.askdirectory(title="Select root storage directory")
        if path:
            self.root_storage_dir.set(path)
            self.refresh_tree()

    def start_download(self):
        if self.is_running:
            return

        set_file = self.set_file_path.get().strip()
        if not set_file or not os.path.isfile(set_file):
            messagebox.showerror("Error", "Please select a valid set list file.")
            return

        root_dir = self.root_storage_dir.get().strip()
        if not root_dir:
            messagebox.showerror("Error", "Please select a root storage directory.")
            return

        # Check write access
        try:
            test_path = Path(root_dir) / "_write_test.tmp"
            ensure_dir(Path(root_dir))
            test_path.write_text("test", encoding="utf-8")
            test_path.unlink(missing_ok=True)
        except Exception as e:
            messagebox.showerror("Error", f"No write access to root directory:\n{e}")
            return

        mysql_cfg = {
            "host": self.mysql_host.get().strip(),
            "user": self.mysql_user.get().strip(),
            "password": self.mysql_password.get().strip(),
            "database": self.mysql_database.get().strip(),
        }

        self.is_running = True
        self.stop_requested = False
        self.start_button.config(state=tk.DISABLED)
        self.verify_button.config(state=tk.DISABLED)
        self.fix_missing_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)

        self.log_text.delete("1.0", tk.END)
        logger.info("Starting download session.")

        concurrency = int(self.concurrency.get())
        skip_completed = bool(self.resume_mode.get())

        thread = threading.Thread(
            target=self._run_async_download,
            args=(set_file, root_dir, mysql_cfg, concurrency, skip_completed),
            daemon=True,
        )
        thread.start()

    def start_verify_only(self):
        self._start_verify_common(fix_missing=False)

    def start_verify_and_fix(self):
        self._start_verify_common(fix_missing=True)

    def _start_verify_common(self, fix_missing: bool):
        if self.is_running:
            return

        root_dir = self.root_storage_dir.get().strip()
        if not root_dir:
            messagebox.showerror("Error", "Please select a root storage directory.")
            return

        mysql_cfg = {
            "host": self.mysql_host.get().strip(),
            "user": self.mysql_user.get().strip(),
            "password": self.mysql_password.get().strip(),
            "database": self.mysql_database.get().strip(),
        }

        self.is_running = True
        self.stop_requested = False
        self.start_button.config(state=tk.DISABLED)
        self.verify_button.config(state=tk.DISABLED)
        self.fix_missing_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)

        self.log_text.delete("1.0", tk.END)
        if fix_missing:
            logger.info("Starting verification + re-download missing session.")
        else:
            logger.info("Starting verification session.")

        concurrency = int(self.concurrency.get())

        thread = threading.Thread(
            target=self._run_async_verify,
            args=(root_dir, mysql_cfg, concurrency, fix_missing),
            daemon=True,
        )
        thread.start()

    def stop_download(self):
        if not self.is_running:
            return
        self.stop_requested = True
        self._log_from_async("Stop requested — finishing current item, no new tasks will start.")

    def refresh_tree(self):
        root_dir = self.root_storage_dir.get().strip()
        self.tree.delete(*self.tree.get_children())
        if not root_dir:
            return
        root_path = Path(root_dir)
        if not root_path.exists():
            return

        for set_dir in sorted(root_path.iterdir()):
            if set_dir.is_dir():
                file_count = len([f for f in set_dir.iterdir() if f.is_file()])
                set_node = self.tree.insert("", "end", text=f"{set_dir.name} ({file_count})", values=("set",), open=False)
                for img_file in sorted(set_dir.iterdir()):
                    if img_file.is_file() and img_file.suffix.lower() in {".jpg", ".jpeg", ".png"}:
                        self.tree.insert(set_node, "end", text=img_file.name, values=("file",))

    def on_tree_select(self, event):
        selected = self.tree.selection()
        if not selected:
            return
        item_id = selected[0]
        parent_id = self.tree.parent(item_id)
        item_text = self.tree.item(item_id, "text")
        parent_text = self.tree.item(parent_id, "text") if parent_id else ""

        values = self.tree.item(item_id, "values")
        if not values or values[0] != "file":
            return

        # parent_text is like "setcode (N)" -> extract setcode
        set_code = parent_text.split(" ", 1)[0]
        file_name = item_text
        root_dir = self.root_storage_dir.get().strip()
        if not root_dir:
            return

        img_path = Path(root_dir) / set_code / file_name
        if not img_path.exists():
            return

        self.show_preview(img_path)
        self.show_card_info(file_name)

    def show_preview(self, img_path: Path):
        try:
            img = Image.open(img_path)
            max_width = 300
            max_height = 420
            img.thumbnail((max_width, max_height), Image.LANCZOS)
            self.preview_image = ImageTk.PhotoImage(img)
            self.preview_label.config(image=self.preview_image, text="")
        except Exception as e:
            self.preview_label.config(text=f"Failed to load image:\n{e}", image="")
            self.preview_image = None

    def show_card_info(self, file_name: str):
        """Look up card info by card_id (derived from filename)."""
        card_id = Path(file_name).stem
        mysql_cfg = {
            "host": self.mysql_host.get().strip(),
            "user": self.mysql_user.get().strip(),
            "password": self.mysql_password.get().strip(),
            "database": self.mysql_database.get().strip(),
        }
        conn = create_mysql_connection(
            mysql_cfg["host"],
            mysql_cfg["user"],
            mysql_cfg["password"],
            mysql_cfg["database"],
        )
        if conn:
            ensure_schema(conn)
            info = get_card_info(conn, card_id)
            conn.close()
        else:
            info = None

        if info:
            self.card_name_var.set(info.get("name") or "-")
            self.card_set_var.set(info.get("set_code") or "-")
            self.card_rarity_var.set(info.get("rarity") or "-")
            self.card_collector_var.set(info.get("collector_number") or "-")
        else:
            self.card_name_var.set("Unknown (no DB entry)")
            self.card_set_var.set("-")
            self.card_rarity_var.set("-")
            self.card_collector_var.set("-")

    # ------------------ Async runners ------------------ #
    def _run_async_download(self, set_file, root_dir, mysql_cfg, concurrency, skip_completed):
        asyncio.run(
            download_all_sets_async(
                set_file,
                root_dir,
                mysql_cfg,
                gui_callbacks={
                    "log_info": self._log_from_async,
                    "reset_overall_progress": self._reset_overall_progress,
                    "update_overall_progress": self._update_overall_progress,
                    "reset_set_progress": self._reset_set_progress,
                    "increment_set_progress": self._increment_set_progress,
                    "set_current_set_label": self._set_current_set_label,
                    "on_all_done": self._on_all_done,
                    "stop_requested": lambda: self.stop_requested,
                },
                concurrency=concurrency,
                skip_completed=skip_completed,
            )
        )
        self.root.after(0, self.refresh_tree)

    def _run_async_verify(self, root_dir, mysql_cfg, concurrency, fix_missing):
        asyncio.run(
            verify_or_fix_all_sets_async(
                root_dir,
                mysql_cfg,
                gui_callbacks={
                    "log_info": self._log_from_async,
                    "reset_overall_progress": self._reset_overall_progress,
                    "update_overall_progress": self._update_overall_progress,
                    "reset_set_progress": self._reset_set_progress,
                    "increment_set_progress": self._increment_set_progress,
                    "set_current_set_label": self._set_current_set_label,
                    "on_all_done": self._on_all_done,
                    "stop_requested": lambda: self.stop_requested,
                },
                concurrency=concurrency,
                fix_missing=fix_missing,
            )
        )
        self.root.after(0, self.refresh_tree)

    # ------------------ GUI callback helpers ------------------ #
    def _log_from_async(self, msg):
        logger.info(msg)

    def _reset_overall_progress(self, total_sets):
        def _():
            self.overall_total_sets = max(total_sets, 1)
            self.overall_progress["value"] = 0
            self.overall_progress["maximum"] = self.overall_total_sets
        self.root.after(0, _)

    def _update_overall_progress(self, completed_sets):
        def _():
            self.overall_progress["value"] = completed_sets
        self.root.after(0, _)

    def _reset_set_progress(self, total_cards):
        def _():
            self.set_total_cards = max(total_cards, 1)
            self.set_progress["value"] = 0
            self.set_progress["maximum"] = self.set_total_cards
        self.root.after(0, _)

    def _increment_set_progress(self):
        def _():
            self.set_progress["value"] = min(
                self.set_progress["value"] + 1, self.set_progress["maximum"]
            )
        self.root.after(0, _)

    def _set_current_set_label(self, text):
        def _():
            self.current_set_label.config(text=f"Current action: {text}")
        self.root.after(0, _)

    def _on_all_done(self):
        def _():
            self.is_running = False
            self.start_button.config(state=tk.NORMAL)
            self.verify_button.config(state=tk.NORMAL)
            self.fix_missing_button.config(state=tk.NORMAL)
            self.stop_button.config(state=tk.DISABLED)
            messagebox.showinfo("Done", "Operation completed.")
        self.root.after(0, _)

    # ------------------ Lifecycle ------------------ #
    def on_close(self):
        cfg = {
            "root_directory": self.root_storage_dir.get().strip(),
            "last_set_file": self.set_file_path.get().strip(),
            "mysql": {
                "host": self.mysql_host.get().strip(),
                "user": self.mysql_user.get().strip(),
                "password": self.mysql_password.get().strip(),
                "database": self.mysql_database.get().strip(),
            },
            "concurrency": int(self.concurrency.get()),
            "resume_mode": bool(self.resume_mode.get()),
        }
        save_config(cfg)
        if self.is_running:
            if not messagebox.askyesno(
                "Quit",
                "A task is still running. Quit anyway?\nThis may leave the current operation incomplete."
            ):
                return
        self.root.destroy()


# -----------------------------
# Main entry point
# -----------------------------
if __name__ == "__main__":
    root = tk.Tk()
    app = MTGDownloaderApp(root)
    root.geometry("1200x750")
    root.mainloop()
