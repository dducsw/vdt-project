#!/usr/bin/env python
"""
run_generator.py — Smart wrapper to seed and run the transaction data generator.

Logic:
1. Connect to PostgreSQL.
2. Check if the 'users' table exists and contains records.
3. If PostgreSQL is empty or tables don't exist:
   - Run 'src/seed_from_csv.py' to initialize the database catalog.
   - Run 'generator.py' with --init-num-users 1000.
4. If PostgreSQL already has data:
   - Skip seeding.
   - Run 'generator.py' with --init-num-users 0 (resume mode).
"""

import os
import sys
import subprocess
import psycopg2
import logging

logging.basicConfig(
    level=logging.INFO, format="[run_generator] %(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("run_generator")

# ---------------------------------------------------------------------------
# Database Configuration
# ---------------------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5434")
PG_DB = os.getenv("PG_DB", "thelook_db")
PG_USER = os.getenv("PG_USER", "db_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "db_password")
PG_SCHEMA = os.getenv("PG_SCHEMA", "demo")
YEAR_SHIFT = os.getenv("YEAR_SHIFT", "4")
AVG_QPS = os.getenv("AVG_QPS", "5")

def check_postgres_has_data() -> bool:
    """
    Connects to PostgreSQL and checks if the schema and 'users' table exist and have records.
    Returns True if data exists, False otherwise.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            connect_timeout=5
        )
        with conn.cursor() as cur:
            # Check if table exists
            cur.execute(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = %s AND table_name = 'users')"
                , (PG_SCHEMA,)
            )
            exists = cur.fetchone()[0]
            if not exists:
                logger.info("Table 'users' does not exist in schema '%s'.", PG_SCHEMA)
                return False

            # Check if table has data
            cur.execute(f"SELECT COUNT(*) FROM {PG_SCHEMA}.users")
            count = cur.fetchone()[0]
            logger.info("Table '%s.users' has %d records.", PG_SCHEMA, count)
            return count > 0
    except Exception as e:
        logger.warning("Could not check PostgreSQL state: %s. Assuming database is empty/needs setup.", e)
        return False
    finally:
        if conn:
            conn.close()

def main():
    logger.info("Checking PostgreSQL state...")
    has_data = check_postgres_has_data()

    if not has_data:
        logger.info("PostgreSQL is empty or uninitialized. Running seed_from_csv...")
        # Run seed_from_csv.py as a subprocess
        seed_cmd = [
            sys.executable,
            "src/seed_from_csv.py",
            "--host", PG_HOST,
            "--port", PG_PORT,
            "--database", PG_DB,
            "--user", PG_USER,
            "--password", PG_PASSWORD,
            "--schema", PG_SCHEMA,
            "--data-dir", "./data",
            "--truncate-first",
            "--year-shift", YEAR_SHIFT,
            "--skip-events"
        ]
        logger.info("Executing seed command: %s", " ".join(seed_cmd))
        res = subprocess.run(seed_cmd)
        if res.returncode != 0:
            logger.error("Database seeding failed. Exiting.")
            sys.exit(res.returncode)
        
        # Fresh initialization gets 1000 users generated
        init_num_users = "1000"
    else:
        logger.info("PostgreSQL already has data. Skipping seeding and resuming generator.")
        # Resume mode does not generate new initial users
        init_num_users = "0"

    # Start the generator.py script
    # We forward database parameters and pass the calculated --init-num-users
    generator_cmd = [
        sys.executable,
        "generator.py",
        "--db-host", PG_HOST,
        "--db-port", PG_PORT,
        "--db-user", PG_USER,
        "--db-password", PG_PASSWORD,
        "--db-name", PG_DB,
        "--db-schema", PG_SCHEMA,
        "--avg-qps", AVG_QPS,
        "--init-num-users", init_num_users,
        "--max-iter", "-1"
    ]

    logger.info("Starting generator: %s", " ".join(generator_cmd))
    
    # Use execvp to replace the current process with the python generator process
    # If on Windows, execvp is supported but we can also use subprocess to keep it simple and portable.
    if sys.platform == "win32":
        res = subprocess.run(generator_cmd)
        sys.exit(res.returncode)
    else:
        os.execvp(generator_cmd[0], generator_cmd)

if __name__ == "__main__":
    main()
