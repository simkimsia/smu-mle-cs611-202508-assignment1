# ETL Pipeline - Running Instructions

GitHub at https://github.com/simkimsia/smu-mle-cs611-202508-assignment1

You can also watch [this](https://www.loom.com/share/d6379bab339a4c0da38c1285561b8056) if you prefer video 🎥 instead.

## Quick Start: Full Pipeline

### Option 1: Using Docker Compose + Jupyter Terminal

1. Start the Jupyter Lab container:

```bash
docker compose up
```

2. Open your browser and go to: `http://localhost:8888`

3. Open a terminal in Jupyter Lab and run:

```bash
python main.py
```

### Option 2: Using Docker Compose ETL Services

```bash
docker-compose --profile etl run etl-full
```

## Running Specific Pipeline Layers

### Using Jupyter Terminal

After running `docker compose up` and accessing Jupyter at `http://localhost:8888`:

**Bronze layer only:**

```bash
python main.py --bronze-only
```

**Silver layer only (requires existing bronze data):**

```bash
python main.py --layer silver --timestamp 20231201_120000
```

**Gold layer only (requires existing silver data):**

```bash
python main.py --layer gold --timestamp 20231201_120000
```

### Using Docker Compose Services

**Bronze layer only:**

```bash
docker-compose --profile etl run etl-bronze
```

**Full pipeline:**

```bash
docker-compose --profile etl run etl-full
```

**Silver layer only:**

```bash
TIMESTAMP=20231201_120000 docker-compose --profile etl run etl-silver
```

**Gold layer only:**

```bash
TIMESTAMP=20231201_120000 docker-compose --profile etl run etl-gold
```

## Notes

- When running silver or gold layers independently, you must provide a `--timestamp` that matches an existing datamart directory
- The timestamp format is: `YYYYMMDD_HHMMSS`
- Running the full pipeline creates a new timestamp automatically
- Output is stored in `datamart/{timestamp}/bronze|silver|gold/`
