# KF2 Panopticon v1.0

English | [简体中文](README_CN.md)

**KF2 Panopticon** is a data aggregation and analytics platform designed for *Killing Floor 2* game servers. This application queries the Steam Master Server API and individual game servers via the A2S protocol to collect real-time data on server status, player activity, and map rotation. It utilizes a SQLite database with a Star Schema design to store historical data and provides a web-based frontend for visualization and analysis.

## System Architecture

The application consists of two primary components:

1. **Data Collector (`Query.py`)**: A multi-threaded Python script responsible for discovering servers, querying their status using A2S (Source Engine Query), and normalizing the data. It handles logic for server identity preservation (dynamic IP handling) and sanitizes server names to group them into "Factions" or operators.
2. **Web Interface (`webapp.py`)**: A Flask-based web application that serves the user interface. It provides views for the global server grid, individual player histories, server details, and statistical reporting.

## Key Features

* **Server Monitoring**: Tracks status, player counts, map rotation, and geographic location for public KF2 servers.
* **Player Analytics**: Records session history, duration, and scores.
* **Note on Identity**: Player tracking is performed based on the player name string returned by the A2S protocol. SteamIDs are not available via this protocol; therefore, if a player changes their name, they are treated as a new unique entity.


* **Associate Detection**: Algorithms analyze session overlaps to identify players who frequently play together.
* **Faction Intelligence**: Categorizes servers into "Factions" or "Operators" (e.g., specific communities or hosting providers) based on regex pattern matching against server names.
* **Data Sanitization**: Implements extensive filtering to strip common keywords (e.g., "Fast DL", "Tickrate") from server names to identify the core server identity.

## Prerequisites

* **Python 3.x**
* **Steam Web API Key**: Required to fetch the master server list from Valve.

### Dependencies

Install the required external Python packages:

```bash
pip install flask requests

```

*Note: The application also utilizes standard libraries including `sqlite3`, `socket`, `struct`, `ipaddress`, `concurrent.futures`, `math`, `time`, `uuid`, `re`, and `datetime`.*

## Configuration

You must configure the file paths and API keys in the source files before running the application.

### 1. Data Collector Configuration (`Query.py`)

Edit `Query.py` and update the following constants:

* **`STEAM_KEY`**: Set this to your valid Steam Web API Key.
* **`DB_FILE`**: Define the absolute path where the SQLite database file will be stored (e.g., `C:\\apps\\Webapp\\kf2_panopticon_v3_star.db`).

### 2. Web Application Configuration (`webapp.py`)

Edit `webapp.py` and update the following constants:

* **`DB_FILE`**: Ensure this path matches the `DB_FILE` path defined in `Query.py`.
* **`app.secret_key`**: (Optional) Update the Flask secret key for session security.

## Usage

### Data Collection

To populate the database, execute the query script. This script acts as the ETL (Extract, Transform, Load) process.

```powershell
python Query.py

```

* This process initializes the database schema if it does not exist.
* It performs a scan of the Steam Master Server list and updates server and player records.
* **Recommendation**: Schedule this script to run at regular intervals (e.g., every 5 to 15 minutes) using Windows Task Scheduler or Cron to maintain historical data.

### Web Interface

To launch the dashboard, execute the web application script.

```powershell
python webapp.py

```

* **Access**: The web server binds to port `9001`. Access the interface via a web browser at `http://127.0.0.1:9001`.

## Technical Notes

* **Dynamic IP Handling**: The system includes logic to detect if a known server (identified by name and configuration) has changed its IP address, allowing for the migration of historical data to the new address.
* **Loopback Handling**: If the collector is run on the same machine as a game server, it attempts to resolve `127.0.0.1` addresses to the public IP to ensure database consistency.
* **Caching**: The web application utilizes an in-memory `DataCache` with a 5-minute Time-To-Live (TTL) to optimize performance for heavy database queries, such as the Faction reports.

## License

This software is provided "as is" without warranty of any kind.

