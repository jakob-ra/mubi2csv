# 🎬 MUBI User Data CSV Export

A Python script to export your **MUBI watchlist** and **ratings** into CSV files, with optional **Letterboxd-compatible** formatting. Perfect for archiving your movie activity or migrating data.  

---

## ⚡ Features

- Export your MUBI **watchlist** (wishes) and **ratings**.  
- Optional **Letterboxd-compatible CSV output** that can be imported directly into Letterboxd reviews or watchlist (see https://letterboxd.com/about/importing-data/) 

---

## 🛠️ Installation

1. Clone the repository:

```bash
git clone https://github.com/jakob-ra/mubi2csv.git
cd mubi2csv
````

2. Install dependencies:

```bash
pip install -r requirements.txt
```

*(Python 3.8+ recommended)*

---

## 🚀 Usage

```bash
python mubi_export.py <user_id> [options]
```

### 🔹 Required

* `user_id` — your MUBI user ID.

### 🔹 Optional Arguments

| Flag               | Description                                                   |
| ------------------ | ------------------------------------------------------------- |
| `--token <token>`  | Bearer token if your account requires authentication.         |
| `--per-page <int>` | Number of items per API request (default: 24).                |
| `--country <code>` | Client country header, e.g., `NL` or `US` (default: NL).      |
| `--letterboxd`     | Generate Letterboxd-compatible CSV alongside the default CSV. |
| `--debug`          | Enable debug output for troubleshooting.                      |

---

### 🔹 Example

```bash
python mubi_export.py 12345678 --letterboxd --debug
```

This will create:

* `mubi_12345678_watchlist.csv`
* `mubi_12345678_ratings.csv`
* Optional: `mubi_12345678_watchlist_letterboxd.csv` & `mubi_12345678_ratings_letterboxd.csv`

---

## 🧹 Output Cleanup

The script automatically:

* Flattens nested JSON fields.
* Converts directors list into a comma-separated string.
* Renames rating columns for general CSV or Letterboxd compatibility.

---

## ⚠️ Notes

* If you hit API rate limits, the script waits and retries automatically.
* Some endpoints may require a **Bearer token**.
* Use `--debug` to troubleshoot API errors.

---

## 📄 License

MIT License — see `LICENSE` for details.
