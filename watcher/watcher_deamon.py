import os
import time
import subprocess
import psycopg2
import re
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- Configuration via Environment Variables ---
DATA_LAKE_DIR = os.environ.get("DATA_LAKE_DIR", "./data_lake")
EXTRACTOR_CMD = os.environ.get("EXTRACTOR_CMD", "./build/extractor")
TARGET_COLUMN = os.environ.get("TARGET_COLUMN", "0")

DB_PARAMS = {
    "dbname": os.environ.get("DB_NAME", "postgres"),
    "user":   os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "password"),
    "host":   os.environ.get("DB_HOST", "localhost"),
    "port":   os.environ.get("DB_PORT", "5432")
}

# Lock to prevent race conditions during DB writes
process_lock = threading.Lock()

# =====================================================================
# Subtask 3.3: GSI Schema Design
# =====================================================================
def initialize_database():
    """Connects to PostgreSQL and initializes the global_index B-Tree schema."""
    print("Initializing PostgreSQL GSI schema...")
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS global_index (
            indexed_value VARCHAR,
            file_path TEXT,
            row_group_id INT
        );
        """
        create_index_query = """
        CREATE INDEX IF NOT EXISTS idx_global_value 
        ON global_index USING btree (indexed_value);
        """
        
        cur.execute(create_table_query)
        cur.execute(create_index_query)
        conn.commit()
        
        cur.close()
        conn.close()
        print("✓ Database schema initialized successfully.")
    except Exception as e:
        print(f"Database Initialization Error: {e}")

# =====================================================================
# Subtask 3.2 & 3.4: Pipeline Hand-off & Index Upsertion
# =====================================================================
def trigger_extraction_engine(file_path):
    """Runs C++, reads the output, and safely inserts into PostgreSQL."""
    with process_lock:
        print(f"\n---> Hand-off triggered for: {file_path}")
        
        try:
            # 1. Run the C++ executable
            subprocess.run(
                [EXTRACTOR_CMD, file_path, TARGET_COLUMN], 
                check=True,
                capture_output=True,
                text=True
            )
            
            # 2. Read the generated text file
            coords_file = "extracted_coordinates.txt"
            if not os.path.exists(coords_file):
                print(f"X No coordinates file generated for {file_path}")
                return

            records = []
            pattern = re.compile(r"\{ value: (.*?), file_path: (.*?), row_group_id: (\d+) \}")
            
            with open(coords_file, "r") as f:
                for line in f:
                    match = pattern.search(line)
                    if match:
                        records.append((match.group(1), match.group(2), int(match.group(3))))

            # 3. Database Updates (Cleanup + Insert)
            if records:
                conn = psycopg2.connect(**DB_PARAMS)
                cur = conn.cursor()
                
                # --- NEW: Delete old records before overwriting ---
                cur.execute("DELETE FROM global_index WHERE file_path = %s;", (file_path,))
                deleted = cur.rowcount
                if deleted > 0:
                    print(f"  -> Cleaned {deleted} old records to prevent duplication.")
                
                # --- Insert new records ---
                insert_query = "INSERT INTO global_index (indexed_value, file_path, row_group_id) VALUES (%s, %s, %s)"
                cur.executemany(insert_query, records)
                
                conn.commit()
                cur.close()
                conn.close()
                print(f"✓ Successfully ingested {len(records)} coordinates into PostgreSQL!")

            # 4. Clean up the text file
            os.remove(coords_file)
            
        except subprocess.CalledProcessError as e:
            print(f"X Extraction failed for {file_path}")
            print(f"Error Details: {e.stderr}")
        except Exception as e:
            print(f"X Database Error: {e}")

# =====================================================================
# Subtask 3.1: Directory Polling
# =====================================================================
class DataLakeHandler(FileSystemEventHandler):
    
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".parquet"):
            trigger_extraction_engine(event.src_path)

    def on_moved(self, event):
        if not event.is_directory and event.dest_path.endswith(".parquet"):
            trigger_extraction_engine(event.dest_path)

    # --- NEW: Handle manual file deletions ---
    def on_deleted(self, event):
        if not event.is_directory and event.src_path.endswith(".parquet"):
            print(f"\nEvent: IN_DELETE detected -> {event.src_path}")
            self.remove_stale_index(event.src_path)

    def remove_stale_index(self, file_path):
        """Deletes all PostgreSQL index records associated with a removed file."""
        with process_lock:
            try:
                conn = psycopg2.connect(**DB_PARAMS)
                cur = conn.cursor()
                cur.execute("DELETE FROM global_index WHERE file_path = %s;", (file_path,))
                deleted_count = cur.rowcount
                conn.commit()
                cur.close()
                conn.close()
                
                if deleted_count > 0:
                    print(f"✓ Cleaned up {deleted_count} stale records for deleted file.")
            except Exception as e:
                print(f"X Database Cleanup Error: {e}")

def start_watcher():
    os.makedirs(DATA_LAKE_DIR, exist_ok=True)
    event_handler = DataLakeHandler()
    observer = Observer()
    observer.schedule(event_handler, DATA_LAKE_DIR, recursive=True)
    observer.start()
    
    print(f"Watcher Daemon actively polling '{DATA_LAKE_DIR}'...\n(Press Ctrl+C to stop)")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nWatcher Daemon stopped gracefully.")
    
    observer.join()

if __name__ == "__main__":
    initialize_database()
    start_watcher()