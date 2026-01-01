1. Install Python 3.10+
2. Have a database (I suggest Mqsql) engine as we use this to store minor card data and as a verification and precess restart source.
3. Install the dependancies using pip:
```powershell
 pip install aiohttp mysql-connector-python
```

4. Create or edit config.json
  They only need to fill in:
    MySQL host
    MySQL user
    MySQL password
    MySQL database name
    Root directory
    Set list file
    Concurrency
    Resume mode
Example:
```json
{
    "root_directory": "D:/MTGImages",
    "last_set_file": "D:/MTG_Data/sets.txt",
    "mysql": {
        "host": "localhost",
        "user": "mtg",
        "password": "mypassword",
        "database": "mtg_images"
    },
    "concurrency": 4,
    "resume_mode": true
}
```

5. Run the script
```bash
 python mtg_downloader.py
```

