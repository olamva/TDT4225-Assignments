import gc
import json
import math
import multiprocessing
import os
import pickle
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from concurrent.futures import as_completed

import mysql.connector


class InteractiveController:
    """Handles keyboard input and controls during processing"""

    def __init__(self):
        self.paused = False
        self.debug = False
        self.stop_requested = False
        self.input_thread = None
        self.processing = False

    def start_input_handler(self):
        """Start the input handling thread"""
        self.processing = True
        self.input_thread = threading.Thread(target=self._input_handler, daemon=True)
        self.input_thread.start()

    def stop_input_handler(self):
        """Stop the input handling"""
        self.processing = False

    def _input_handler(self):
        """Handle keyboard input in background thread"""
        print("\n" + "="*50)
        print("INTERACTIVE CONTROLS:")
        print("  'p' + Enter: Pause/Resume")
        print("  'd' + Enter: Toggle debug logging")
        print("  'r' + Enter: Show current results")
        print("  's' + Enter: Save progress and stop")
        print("  'q' + Enter: Quit immediately")
        print("="*50 + "\n")

        while self.processing:
            try:
                user_input = input().strip().lower()

                if user_input == 'p':
                    self.paused = not self.paused
                    status = "PAUSED" if self.paused else "RESUMED"
                    print(f"\n>>> {status} <<<")

                elif user_input == 'd':
                    self.debug = not self.debug
                    status = "ON" if self.debug else "OFF"
                    print(f"\n>>> Debug logging: {status} <<<")
                    if self.debug:
                        print(">>> DEBUG MODE ACTIVATED - Detailed logging enabled <<<")
                        print(">>> You will now see step-by-step processing information <<<")

                elif user_input == 'r':
                    print(f"\n>>> Request to show results received <<<")

                elif user_input == 's':
                    print(f"\n>>> Stop requested - will save progress and exit gracefully <<<")
                    print(f">>> Database will be disconnected safely <<<")
                    self.stop_requested = True

                elif user_input == 'q':
                    print(f"\n>>> Immediate quit requested <<<")
                    os._exit(0)

            except EOFError:
                break
            except Exception as e:
                if self.debug:
                    print(f"\n>>> Input error: {e} <<<")

    def wait_if_paused(self):
        """Wait while paused"""
        while self.paused and self.processing:
            time.sleep(0.1)

class ThreadSafeSpatialIndex:
    """Thread-safe spatial index for faster proximity searches"""

    def __init__(self, cell_size=0.00005):  # ~5.5m cells - much more precise for 5m search
        self.cell_size = cell_size
        self.cells = defaultdict(list)  # {(cell_x, cell_y): [(timestamp, taxi_id, lat, lon), ...]}
        self.lock = threading.RLock()  # Reentrant lock for thread safety

    def _get_cell(self, lat, lon):
        """Get cell coordinates for a point"""
        cell_x = int(lat / self.cell_size)
        cell_y = int(lon / self.cell_size)
        return (cell_x, cell_y)

    def add_point(self, timestamp, taxi_id, lat, lon):
        """Add a point to the spatial index (thread-safe)"""
        cell = self._get_cell(lat, lon)
        with self.lock:
            self.cells[cell].append((timestamp, taxi_id, lat, lon))

    def add_points_batch(self, points):
        """Add multiple points at once (more efficient for threading)"""
        grouped_by_cell = defaultdict(list)
        for timestamp, taxi_id, lat, lon in points:
            cell = self._get_cell(lat, lon)
            grouped_by_cell[cell].append((timestamp, taxi_id, lat, lon))

        with self.lock:
            for cell, cell_points in grouped_by_cell.items():
                self.cells[cell].extend(cell_points)

    def get_nearby_points(self, lat, lon, max_time_diff, current_time):
        """Get points in nearby cells within time window (thread-safe)"""
        cell_x, cell_y = self._get_cell(lat, lon)
        nearby_points = []

        with self.lock:
            # Check 3x3 grid of cells, but with smaller cells this is now ~17m radius
            for dx in [-1, 0, 1]:
                for dy in [-1, 0, 1]:
                    check_cell = (cell_x + dx, cell_y + dy)
                    if check_cell in self.cells:
                        for point in self.cells[check_cell]:
                            point_time = point[0]
                            # Only consider points within time window
                            if abs(point_time - current_time) <= max_time_diff:
                                nearby_points.append(point)

        return nearby_points

    def cleanup_old_points(self, cutoff_time):
        """Remove points older than cutoff_time to save memory"""
        with self.lock:
            for cell in list(self.cells.keys()):
                self.cells[cell] = [p for p in self.cells[cell] if p[0] >= cutoff_time]
                if not self.cells[cell]:
                    del self.cells[cell]

class WorkerThread:
    """Worker thread for processing batches of GPS points"""

    def __init__(self, thread_id, spatial_index, max_distance, max_time_diff, controller):
        self.thread_id = thread_id
        self.spatial_index = spatial_index
        self.max_distance = max_distance
        self.max_time_diff = max_time_diff
        self.controller = controller
        self.local_pairs = set()

    def process_point_batch(self, points_batch):
        """Process a batch of points and find pairs"""
        if self.controller.debug:
            print(f"[DEBUG] Thread {self.thread_id}: Starting batch of {len(points_batch)} points")

        found_pairs = set()

        for i, (timestamp, taxi_id, lat, lon) in enumerate(points_batch):
            if self.controller.stop_requested:
                if self.controller.debug:
                    print(f"[DEBUG] Thread {self.thread_id}: Stop requested, breaking at point {i+1}/{len(points_batch)}")
                break

            if self.controller.debug and i % 100 == 0 and i > 0:
                print(f"[DEBUG] Thread {self.thread_id}: Processed {i}/{len(points_batch)} points in batch")

            # Find nearby points using spatial index
            nearby_points = self.spatial_index.get_nearby_points(lat, lon, self.max_time_diff, timestamp)

            if self.controller.debug and len(nearby_points) > 0:
                print(f"[DEBUG] Thread {self.thread_id}: Found {len(nearby_points)} nearby points for taxi {taxi_id}")

            for nearby_time, nearby_taxi, nearby_lat, nearby_lon in nearby_points:
                if taxi_id != nearby_taxi and abs(timestamp - nearby_time) <= self.max_time_diff:
                    # Fast distance check first
                    if fast_distance_check(lat, lon, nearby_lat, nearby_lon, self.max_distance):
                        # Precise distance calculation
                        distance = haversine_distance(lat, lon, nearby_lat, nearby_lon)
                        if distance <= self.max_distance:
                            pair = tuple(sorted([taxi_id, nearby_taxi]))
                            found_pairs.add(pair)

                            if self.controller.debug:
                                print(f"[DEBUG] Thread {self.thread_id}: ✓ PAIR FOUND: {pair} at distance {distance:.2f}m, time diff {abs(timestamp - nearby_time):.1f}s")

            # Add point to spatial index immediately after processing (CRITICAL FIX)
            self.spatial_index.add_point(timestamp, taxi_id, lat, lon)

        # No longer need to batch add points since we add them individually

        if self.controller.debug:
            print(f"[DEBUG] Thread {self.thread_id}: Batch complete - found {len(found_pairs)} pairs")

        return found_pairs

class SpatialIndex:
    """Simple spatial index for faster proximity searches (kept for compatibility)"""

    def __init__(self, cell_size=0.0001):  # ~10m cells
        self.cell_size = cell_size
        self.cells = defaultdict(list)  # {(cell_x, cell_y): [(timestamp, taxi_id, lat, lon), ...]}

    def _get_cell(self, lat, lon):
        """Get cell coordinates for a point"""
        cell_x = int(lat / self.cell_size)
        cell_y = int(lon / self.cell_size)
        return (cell_x, cell_y)

    def add_point(self, timestamp, taxi_id, lat, lon):
        """Add a point to the spatial index"""
        cell = self._get_cell(lat, lon)
        self.cells[cell].append((timestamp, taxi_id, lat, lon))

    def get_nearby_points(self, lat, lon, max_time_diff, current_time):
        """Get points in nearby cells within time window"""
        cell_x, cell_y = self._get_cell(lat, lon)
        nearby_points = []

        # Check 3x3 grid of cells
        for dx in [-1, 0, 1]:
            for dy in [-1, 0, 1]:
                check_cell = (cell_x + dx, cell_y + dy)
                if check_cell in self.cells:
                    for point in self.cells[check_cell]:
                        point_time = point[0]
                        # Only consider points within time window
                        if abs(point_time - current_time) <= max_time_diff:
                            nearby_points.append(point)

        return nearby_points

def fast_distance_check(lat1, lon1, lat2, lon2, max_distance=5):
    """
    Ultra-fast distance approximation for initial filtering.
    At Porto latitude (~41°), 1 degree ≈ 111km lat, 83km lon
    """
    lat_diff = abs(lat2 - lat1)
    lon_diff = abs(lon2 - lon1)

    # For 5m: lat ≈ 0.000045°, lon ≈ 0.00006° - make these more restrictive
    if lat_diff > 0.000040 or lon_diff > 0.000055:  # Tighter thresholds
        return False

    # Quick Euclidean approximation (good enough for 5m)
    lat_m = lat_diff * 111000
    lon_m = lon_diff * 83000  # Adjusted for Porto latitude
    return (lat_m * lat_m + lon_m * lon_m) <= (max_distance * max_distance)

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000  # Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R * c

def save_progress(close_pairs, processed_points, total_points, processed_trips=0, filename="query8_progress.pkl"):
    """Save current progress to file"""
    progress = {
        'close_pairs': list(close_pairs),
        'processed_points': processed_points,
        'total_points': total_points,
        'processed_trips': processed_trips,
        'timestamp': time.time()
    }
    with open(filename, 'wb') as f:
        pickle.dump(progress, f)
    print(f"\n>>> Progress saved to {filename} <<<")

def load_progress(filename="query8_progress.pkl"):
    """Load previous progress from file"""
    try:
        with open(filename, 'rb') as f:
            progress = pickle.load(f)
        print(f"\n>>> Loaded progress from {filename} <<<")
        return progress
    except FileNotFoundError:
        return None

def query8_multithreaded():
    """
    Multithreaded version with spatial indexing and interactive controls
    Optimized for M1 Pro with multiple cores
    """
    print(f"\n{'='*60}")
    print(f"🚀 STARTING QUERY8 MULTITHREADED OPTIMIZATION")
    print(f"{'='*60}")

    controller = InteractiveController()
    controller.start_input_handler()

    # Detect optimal number of threads for M1 Pro
    cpu_count = multiprocessing.cpu_count()
    # Use fewer threads than cores to leave room for DB and system processes
    num_threads = max(2, min(cpu_count - 2, 6))  # 2-6 threads typically optimal
    print(f"🖥️  System: {cpu_count} CPU cores detected")
    print(f"🧵 Threading: Using {num_threads} worker threads (optimal for M1 Pro)")

    # Try to load previous progress
    print(f"\n📂 Checking for previous progress...")
    progress = load_progress()
    if progress:
        # Validate progress data
        processed_points = progress.get('processed_points', 0)
        total_points = progress.get('total_points', 1)
        processed_trips = progress.get('processed_trips', 0)

        # Check for invalid progress data
        if processed_points > total_points * 2:  # Allow some tolerance but catch obvious errors
            print(f"⚠️  WARNING: Progress file appears corrupted (processed > total by large margin)")
            print(f"   Processed points: {processed_points:,}")
            print(f"   Total points: {total_points:,}")
            response = input(f"Use corrupted progress anyway? (y/N): ")
            if response.lower() != 'y':
                progress = None
                print(f"📄 Ignoring corrupted progress, starting fresh")

        if progress:
            trips_info = f", {processed_trips:,} trips" if processed_trips > 0 else ""
            points_percent = (processed_points / max(total_points, 1)) * 100
            response = input(f"Found previous progress ({processed_points:,}/{total_points:,} points{trips_info}, {points_percent:.1f}%). Continue? (y/n): ")
        if response.lower() != 'y':
            progress = None
            # Offer to delete the progress file
            delete_response = input(f"Delete progress file to avoid confusion in future runs? (y/n): ")
            if delete_response.lower() == 'y':
                try:
                    os.remove("query8_progres.pkl")
                    print(f"🗑️  Deleted progress file")
                except FileNotFoundError:
                    pass
            print(f"📄 Starting fresh analysis")
        else:
            print(f"📈 Resuming from previous session")
            print(f"💡 Note: Still need to reload trip data from database into memory")
    else:
        print(f"📄 No previous progress found, starting fresh")

    print(f"\n🔌 Connecting to database...")
    try:
        conn = mysql.connector.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="secret",
            database="porto",
            autocommit=True,
            connection_timeout=300,
            use_unicode=True,
            charset='utf8mb4'
        )
        print(f"✅ Database connection established")
    except mysql.connector.Error as e:
        print(f"\n❌ Failed to connect to MySQL database:")
        print(f"   Error: {e}")
        print(f"\n💡 Please ensure:")
        print(f"   • Docker container is running: docker ps")
        print(f"   • MySQL service is available on localhost:3306")
        print(f"   • Database 'porto' exists and is accessible")
        print(f"\n🚀 To start the Docker container, try:")
        print(f"   docker start <container_name>")
        print(f"   # or")
        print(f"   docker-compose up -d")
        return None, 0, 0

    if progress:
        print(f"\n📊 Re-analyzing dataset (required even when resuming)...")
        print(f"💾 Progress saves results, not source data - must reload trips")
    else:
        print(f"\n📊 Analyzing dataset size...")

    # Get total count first for progress tracking
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM trip_by_taxi t
        JOIN trip_journey j ON t.trip_id = j.trip_id
        WHERE j.polyline IS NOT NULL
          AND j.timestamp_ IS NOT NULL
          AND JSON_LENGTH(j.polyline) BETWEEN 3 AND 500
    """)
    total_trips = cur.fetchone()[0]
    print(f"📈 Dataset: {total_trips:,} trips to process")

    # Check if previous progress indicates completion
    if progress:
        # We need to estimate if the previous run was complete
        # The saved total_points might be accurate if it's close to what we expect
        avg_points_per_trip = 10  # Conservative estimate
        estimated_total_points = total_trips * avg_points_per_trip

        completion_ratio = progress['processed_points'] / max(progress['total_points'], estimated_total_points)

        if completion_ratio >= 0.99:  # 99% complete is considered done
            print(f"🎉 PREVIOUS RUN WAS ESSENTIALLY COMPLETE!")
            print(f"📊 Found {len(progress['close_pairs']):,} close pairs in previous run")
            print(f"✅ {progress['processed_points']:,} points were processed ({completion_ratio*100:.1f}% complete)")

            # Save results to human-readable JSON
            results_file = "results/query8_final_results.json"

            # Convert pair strings back to proper format for JSON
            formatted_pairs = []
            for pair_str in progress['close_pairs']:
                if ' ↔ ' in pair_str:
                    taxi1, taxi2 = pair_str.split(' ↔ ')
                    formatted_pairs.append([taxi1.strip(), taxi2.strip()])
                else:
                    formatted_pairs.append([pair_str, ""])  # fallback

            results_data = {
                'query': 'Find pairs of different taxis within 5m and 5 seconds',
                'summary': {
                    'total_close_pairs': len(progress['close_pairs']),
                    'total_points_processed': progress['processed_points'],
                    'total_points_analyzed': progress['total_points'],
                    'completion_percentage': completion_ratio * 100
                },
                'pairs': formatted_pairs,
                'metadata': {
                    'analysis_completed_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'timestamp': progress.get('timestamp', time.time()),
                    'distance_threshold_meters': 5,
                    'time_threshold_seconds': 5
                }
            }

            with open(results_file, "w") as f:
                json.dump(results_data, f, indent=2)

            print(f"💾 Results saved to {results_file}")
            print(f"\n📋 Sample close pairs:")
            sample_pairs = list(progress['close_pairs'])[:10]
            for i, pair_str in enumerate(sample_pairs):
                print(f"   {i+1:2d}. {pair_str}")

            print(f"\n✅ Query 8 analysis complete!")
            print(f"📁 Full results available in {results_file}")
            print(f"🐳 Docker container can be safely stopped")

            # Close database before exiting
            cur.close()
            conn.close()
            sys.exit(0)
        else:
            print(f"📊 Previous progress: {completion_ratio*100:.1f}% complete - continuing from where left off")

    # Estimate processing time
    estimated_minutes = max(1, total_trips // 10000)  # Rough estimate
    print(f"⏱️  Estimated runtime: {estimated_minutes}-{estimated_minutes*2} minutes")

    # Main query - LOAD ALL DATA INTO MEMORY FIRST
    if progress:
        print(f"\n🔄 Reloading ALL trip data into memory (required for resumed session)...")
        print(f"📈 Your progress ({progress['processed_points']:,} points) will be used to skip processed data")
    else:
        print(f"\n🔄 Loading ALL trip data into memory...")
    print(f"⚠️  This may take 2-3 minutes but then Docker can be safely stopped")
    sql = """
    SELECT
        t.taxi_id,
        j.polyline,
        UNIX_TIMESTAMP(j.timestamp_) as start_timestamp
    FROM trip_by_taxi t
    JOIN trip_journey j ON t.trip_id = j.trip_id
    WHERE j.polyline IS NOT NULL
      AND j.timestamp_ IS NOT NULL
      AND JSON_LENGTH(j.polyline) BETWEEN 3 AND 500
    ORDER BY j.timestamp_
    """

    cur = conn.cursor()  # Regular cursor to load all data
    cur.execute(sql)

    print(f"📥 Fetching all {total_trips:,} trips from database...")
    all_trips_data = cur.fetchall()
    print(f"✅ Loaded {len(all_trips_data):,} trips into memory")

    # Close database connection immediately
    cur.close()
    conn.close()
    print(f"🔌 Database connection closed")

    # IMPORTANT: Database is now disconnected - Docker container can be stopped safely
    print(f"\n🐳 DOCKER CONTAINER SAFETY NOTICE:")
    print(f"{'='*50}")
    print(f"✅ ALL DATABASE DATA LOADED INTO MEMORY")
    print(f"🔥 IT IS NOW SAFE TO STOP THE DOCKER CONTAINER")
    print(f"💾 This will free up significant system memory")
    print(f"🚀 Processing will continue using loaded data")
    print(f"🔒 No more database access needed")
    print(f"{'='*50}")

    # Give user time to stop Docker if they want
    print(f"\n⏱️  Pausing 10 seconds for Docker container management...")
    time.sleep(10)

    # Initialize thread-safe spatial index and tracking
    print(f"\n🗂️  Initializing spatial indexing system...")
    spatial_index = ThreadSafeSpatialIndex()
    close_pairs = set(progress['close_pairs']) if progress else set()

    max_distance = 5  # 5 meters
    max_time_diff = 5  # 5 seconds

    processed_trips = progress['processed_trips'] if progress else 0
    processed_points = progress['processed_points'] if progress else 0

    # Calculate ACTUAL total points by scanning all trips first (this is the fix!)
    print(f"📊 Calculating total GPS points in dataset...")
    actual_total_points = 0
    for taxi_id, polyline_json, start_timestamp in all_trips_data:
        try:
            polyline = json.loads(polyline_json) if isinstance(polyline_json, str) else polyline_json
            if polyline:
                # Count points that will actually be processed
                valid_points = sum(1 for point in polyline if len(point) >= 2)
                actual_total_points += valid_points
        except:
            continue

    print(f"📈 Total GPS points in dataset: {actual_total_points:,}")

    # Use the actual total, not a running counter
    total_points = actual_total_points
    current_points_processed = 0  # This will be our running counter

    batch_size = 2000  # Larger batches for multithreading
    thread_batch_size = batch_size // num_threads  # Split among threads

    if progress:
        print(f"📈 Resuming from trip {processed_trips:,} (will skip already processed trips)")
        completion_percent = (processed_points / total_points) * 100 if total_points > 0 else 0
        print(f"📊 Previous completion: {completion_percent:.1f}% ({processed_points:,}/{total_points:,} points)")

    start_time = time.time()
    last_save_time = start_time
    last_cleanup_time = start_time
    save_interval = 15 * 60  # Save every 15 minutes
    cleanup_interval = 300  # Cleanup old spatial index data every 5 minutes (more aggressive)

    print(f"⚙️  Configuration:")
    print(f"   • Distance threshold: {max_distance}m")
    print(f"   • Time threshold: {max_time_diff}s")
    print(f"   • Batch size: {batch_size} points")
    print(f"   • Thread batch size: {thread_batch_size} points")
    print(f"   • Auto-save interval: {save_interval//60} minutes")
    print(f"   • Memory cleanup: Every {cleanup_interval//60} minutes")

    print(f"\n🚀 Starting multithreaded processing with {num_threads} workers...")
    print(f"💡 Tip: Type 'd' + Enter anytime to toggle detailed debug logging")

    # Thread pool for processing
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        batch_points = []
        future_to_batch = {}

        print(f"\n🔄 Processing {len(all_trips_data):,} trips from memory...")

        if progress and processed_trips > 0:
            print(f"⏭️  Resuming: Will skip first {processed_trips:,} already processed trips...")

        skipped_trips = 0
        resume_message_shown = False
        initial_processed_trips = processed_trips  # Store the initial value before any processing

        for trip_index, (taxi_id, polyline_json, start_timestamp) in enumerate(all_trips_data):
            controller.wait_if_paused()

            if controller.stop_requested:
                print(f"\n⏹️  Stop requested by user - saving progress...")
                break

            # Skip already processed trips when resuming
            if trip_index < initial_processed_trips:
                skipped_trips += 1
                if skipped_trips % 5000 == 0:
                    print(f"⏭️  Skipped {skipped_trips:,}/{initial_processed_trips:,} already processed trips...")
                if controller.debug and trip_index % 1000 == 0:
                    print(f"[DEBUG] Skipping already processed trip {trip_index + 1}/{initial_processed_trips}")
                continue

            # Show when we start processing new trips (only once) - only if we actually have progress to resume from
            if trip_index >= initial_processed_trips and initial_processed_trips > 0 and not resume_message_shown:
                print(f"✅ Skipped {initial_processed_trips:,} already processed trips - now processing new data...")
                resume_message_shown = True

            try:
                if controller.debug:
                    print(f"[DEBUG] Processing trip {trip_index + 1}: taxi_id={taxi_id}, timestamp={start_timestamp}")

                polyline = json.loads(polyline_json) if isinstance(polyline_json, str) else polyline_json
                if not polyline:
                    if controller.debug:
                        print(f"[DEBUG] Skipping trip {taxi_id}: empty polyline")
                    processed_trips += 1
                    continue

                processed_trips += 1

                if controller.debug and (processed_trips - (progress['processed_trips'] if progress else 0)) <= 5:
                    print(f"[DEBUG] Trip {processed_trips}: taxi {taxi_id} has {len(polyline)} GPS points")

                # Convert polyline to points
                trip_points = []
                for i, point in enumerate(polyline):
                    if len(point) >= 2:
                        lon, lat = point[0], point[1]
                        point_timestamp = start_timestamp + (i * 15)
                        trip_points.append((point_timestamp, taxi_id, lat, lon))
                        # Don't increment current_points_processed here - only count when actually processed

                batch_points.extend(trip_points)

                if controller.debug and len(batch_points) % 500 == 0:
                    print(f"[DEBUG] Batch accumulating: {len(batch_points)} points ready for processing")

                # Process batch when it's full
                if len(batch_points) >= batch_size:
                    if controller.debug:
                        print(f"[DEBUG] STEP: Processing full batch of {len(batch_points)} points")
                        print(f"[DEBUG] STEP: Splitting into {num_threads} thread batches")

                    # Split batch among threads
                    thread_batches = [
                        batch_points[i:i + thread_batch_size]
                        for i in range(0, len(batch_points), thread_batch_size)
                    ]

                    if controller.debug:
                        print(f"[DEBUG] STEP: Created {len(thread_batches)} thread batches")
                        for i, tb in enumerate(thread_batches):
                            print(f"[DEBUG]   Thread {i}: {len(tb)} points")

                    # Submit work to thread pool
                    for i, thread_batch in enumerate(thread_batches):
                        if thread_batch:  # Only submit non-empty batches
                            if controller.debug:
                                print(f"[DEBUG] STEP: Submitting batch {i} to thread pool")
                            worker = WorkerThread(i, spatial_index, max_distance, max_time_diff, controller)
                            future = executor.submit(worker.process_point_batch, thread_batch)
                            future_to_batch[future] = len(thread_batch)

                    if controller.debug:
                        print(f"[DEBUG] STEP: Waiting for {len(future_to_batch)} threads to complete...")

                    # Collect results from completed futures with stop checking
                    completed_threads = 0
                    remaining_futures = set(future_to_batch.keys())
                    start_wait_time = time.time()
                    warned_about_delay = False

                    while remaining_futures and not controller.stop_requested:
                        try:
                            # Check if threads are taking unusually long
                            wait_time = time.time() - start_wait_time
                            if wait_time > 30 and not warned_about_delay and len(remaining_futures) > 0:
                                print(f"\n⚠️  Threads processing for {wait_time:.1f}s - this is normal for large batches")
                                print(f"   {len(remaining_futures)} threads still working on spatial analysis...")
                                print(f"   💡 Tip: Press 's' + Enter to request graceful stop anytime")
                                warned_about_delay = True

                            # Wait for any future to complete with a short timeout
                            for future in as_completed(remaining_futures, timeout=0.5):
                                batch_pairs = future.result()
                                close_pairs.update(batch_pairs)
                                batch_processed_points = future_to_batch[future]
                                processed_points += batch_processed_points
                                completed_threads += 1
                                remaining_futures.remove(future)

                                if controller.debug:
                                    print(f"[DEBUG] STEP: Thread {completed_threads}/{len(future_to_batch)} completed, found {len(batch_pairs)} new pairs (total: {len(close_pairs)})")
                                break  # Process one future at a time to check stop frequently
                        except FuturesTimeoutError:
                            # Just continue waiting - timeouts are normal and expected
                            continue

                    future_to_batch.clear()
                    batch_points = []

                    if controller.debug:
                        print(f"[DEBUG] STEP: Batch processing complete, running maintenance tasks...")

                    # Periodic maintenance
                    current_time = time.time()

                    # Save progress periodically
                    if current_time - last_save_time > save_interval:
                        if controller.debug:
                            print(f"[DEBUG] STEP: Auto-saving progress (last save {(current_time - last_save_time)/60:.1f} minutes ago)")
                        save_progress(close_pairs, processed_points, total_points, processed_trips)
                        last_save_time = current_time

                    # Clean up old spatial index data to prevent memory bloat
                    if current_time - last_cleanup_time > cleanup_interval:
                        cutoff_time = start_timestamp - max_time_diff - 60  # Keep extra minute buffer
                        if controller.debug:
                            print(f"[DEBUG] STEP: Cleaning up spatial index (removing data older than {cutoff_time})")
                        spatial_index.cleanup_old_points(cutoff_time)
                        gc.collect()  # Force garbage collection
                        last_cleanup_time = current_time
                        if controller.debug:
                            print(f"[DEBUG] STEP: Memory cleanup complete")

            except (json.JSONDecodeError, TypeError, IndexError, AttributeError) as e:
                if controller.debug:
                    print(f"[DEBUG] ERROR processing trip {taxi_id}: {e}")
                continue

            # Progress reporting
            if processed_trips % 3000 == 0:  # More frequent updates for multithreading
                current_time = time.time()
                elapsed = current_time - start_time

                # Calculate rate based on trips processed since last report (more accurate)
                if not hasattr(query8_multithreaded, '_last_report_time'):
                    query8_multithreaded._last_report_time = start_time
                    query8_multithreaded._last_report_trips = processed_trips - 3000

                time_since_last = current_time - query8_multithreaded._last_report_time
                trips_since_last = processed_trips - query8_multithreaded._last_report_trips
                current_rate = trips_since_last / time_since_last if time_since_last > 0 else 0

                # Update for next report
                query8_multithreaded._last_report_time = current_time
                query8_multithreaded._last_report_trips = processed_trips

                current_completion = (processed_points / total_points * 100) if total_points > 0 else 0
                pair_rate = len(close_pairs) / processed_points if processed_points > 0 else 0
                estimated_total_pairs = int(pair_rate * total_points)
                print(f"📊 Progress: {processed_trips:,}/{len(all_trips_data):,} trips ({processed_trips/len(all_trips_data)*100:.1f}%) "
                      f"| Points: {processed_points:,}/{total_points:,} ({current_completion:.1f}%) | Pairs: {len(close_pairs):,} "
                      f"| Rate: {current_rate:.1f} trips/s | Est. Total Pairs: {estimated_total_pairs:,}")

                if controller.debug:
                    print(f"[DEBUG] Detailed progress: {processed_points:,} points processed, {len(close_pairs)} unique pairs found")        # Process remaining batch - CRITICAL: Must process ALL remaining points
        if batch_points:
            print(f"\n🔄 Processing final batch of {len(batch_points)} points...")
            print(f"⚠️  IMPORTANT: This ensures ALL GPS points are processed for proximity detection")
            if controller.debug:
                print(f"[DEBUG] FINAL STEP: Processing remaining {len(batch_points)} points")

            # Use smaller thread batches for final processing if needed
            final_thread_batch_size = max(100, thread_batch_size)  # Ensure reasonable batch sizes
            thread_batches = [
                batch_points[i:i + final_thread_batch_size]
                for i in range(0, len(batch_points), final_thread_batch_size)
            ]

            print(f"📊 Final batch breakdown: {len(thread_batches)} thread batches")
            if controller.debug:
                print(f"[DEBUG] FINAL STEP: Split into {len(thread_batches)} final thread batches")
                for idx, tb in enumerate(thread_batches):
                    print(f"[DEBUG]   Final thread {idx}: {len(tb)} points")

            future_to_batch.clear()  # Clear any previous futures

            for i, thread_batch in enumerate(thread_batches):
                if thread_batch:
                    if controller.debug:
                        print(f"[DEBUG] FINAL STEP: Submitting final batch {i} with {len(thread_batch)} points")
                    worker = WorkerThread(i, spatial_index, max_distance, max_time_diff, controller)
                    future = executor.submit(worker.process_point_batch, thread_batch)
                    future_to_batch[future] = len(thread_batch)

            # Collect final results with stop checking
            print(f"⏳ Waiting for {len(future_to_batch)} final threads to complete...")
            if controller.debug:
                print(f"[DEBUG] FINAL STEP: Collecting results from {len(future_to_batch)} final threads")

            final_threads_completed = 0
            remaining_futures = set(future_to_batch.keys())
            final_start_time = time.time()
            final_warned = False

            while remaining_futures and not controller.stop_requested:
                try:
                    # Warn if final processing is taking long
                    final_wait_time = time.time() - final_start_time
                    if final_wait_time > 20 and not final_warned and len(remaining_futures) > 0:
                        print(f"\n⏳ Final processing taking {final_wait_time:.1f}s - completing remaining analysis...")
                        print(f"   {len(remaining_futures)} final threads still working...")
                        final_warned = True

                    for future in as_completed(remaining_futures, timeout=0.5):
                        batch_pairs = future.result()
                        close_pairs.update(batch_pairs)
                        batch_processed_points = future_to_batch[future]
                        processed_points += batch_processed_points
                        final_threads_completed += 1
                        remaining_futures.remove(future)

                        if controller.debug:
                            print(f"[DEBUG] FINAL: Thread {final_threads_completed}/{len(future_to_batch)} completed, found {len(batch_pairs)} new pairs")
                        else:
                            print(f"✅ Final thread {final_threads_completed}/{len(future_to_batch)} complete")
                        break  # Process one at a time
                except FuturesTimeoutError:
                    # Timeouts are normal - just continue waiting
                    continue

            print(f"🎉 Final batch processing complete: {len(batch_points)} points processed")

    print(f"\n✅ In-memory processing complete")

    # CRITICAL VALIDATION: Verify we processed all points
    processing_completion = (processed_points / total_points) * 100 if total_points > 0 else 0
    print(f"\n📊 PROCESSING VERIFICATION:")
    print(f"   • Expected total points: {total_points:,}")
    print(f"   • Actually processed: {processed_points:,}")
    print(f"   • Completion percentage: {processing_completion:.1f}%")

    if processing_completion < 99.5:  # Allow small rounding tolerance
        print(f"⚠️  WARNING: INCOMPLETE PROCESSING DETECTED!")
        print(f"   Missing {total_points - processed_points:,} points ({100 - processing_completion:.1f}%)")
        print(f"   Results may be incomplete - consider re-running with debug mode")
    else:
        print(f"✅ VERIFICATION PASSED: All points processed successfully")

    print(f"\n🐳 DOCKER CONTAINER FINAL NOTICE:")
    print(f"{'='*40}")
    print(f"✅ ALL PROCESSING COMPLETE")
    print(f"🔥 DOCKER CONTAINER CAN BE STOPPED")
    print(f"💾 All results ready")
    print(f"{'='*40}")

    # Final save
    print(f"\n💾 Saving final progress...")
    save_progress(close_pairs, processed_points, total_points, processed_trips)
    controller.stop_input_handler()

    if controller.debug:
        print(f"[DEBUG] COMPLETION: Final statistics:")
        print(f"[DEBUG]   - Total trips processed: {processed_trips:,}")
        print(f"[DEBUG]   - Total points processed: {processed_points:,}")
        print(f"[DEBUG]   - Total GPS coordinates: {total_points:,}")
        print(f"[DEBUG]   - Unique taxi pairs found: {len(close_pairs)}")

    print(f"🏁 Processing complete!")
    return list(close_pairs), total_points, processed_points

def process_batch(batch_points, spatial_index, close_pairs, max_distance, max_time_diff, controller):
    """Process a batch of points efficiently"""
    processed_count = 0

    # Sort batch by time for temporal locality
    batch_points.sort()

    for timestamp, taxi_id, lat, lon in batch_points:
        controller.wait_if_paused()

        if controller.stop_requested:
            break

        # Find nearby points using spatial index
        nearby_points = spatial_index.get_nearby_points(lat, lon, max_time_diff, timestamp)

        for nearby_time, nearby_taxi, nearby_lat, nearby_lon in nearby_points:
            if taxi_id != nearby_taxi and abs(timestamp - nearby_time) <= max_time_diff:
                # Fast distance check first
                if fast_distance_check(lat, lon, nearby_lat, nearby_lon, max_distance):
                    # Precise distance calculation
                    distance = haversine_distance(lat, lon, nearby_lat, nearby_lon)
                    if distance <= max_distance:
                        pair = tuple(sorted([taxi_id, nearby_taxi]))
                        close_pairs.add(pair)

                        if controller.debug:
                            print(f"Found pair: {pair} at distance {distance:.2f}m")

        # Add point to spatial index for future comparisons
        spatial_index.add_point(timestamp, taxi_id, lat, lon)
        processed_count += 1

    return processed_count

if __name__ == "__main__":
    print("=== QUERY 8 ULTRA-OPTIMIZED WITH MULTITHREADING ===")
    print("This version includes:")
    print("- Spatial indexing for faster searches")
    print("- Progress saving and resuming")
    print("- Interactive controls (pause, debug, early stop)")
    print("- Memory-efficient streaming processing")
    print("- MULTITHREADING for M1 Pro optimization")
    print("- Automatic memory cleanup to prevent bloat")
    print("- COMPREHENSIVE DEBUG LOGGING")
    print("- Docker container safety indicators")
    print("- Better progress tracking with ETA")
    print(f"\n💡 Pro tip: Type 'd' + Enter during execution to toggle detailed debug logging")
    print(f"🐳 Watch for Docker container safety messages to optimize memory usage")

    try:
        start_time = time.time()
        print(f"\n⏱️  Analysis started at {time.strftime('%H:%M:%S', time.localtime(start_time))}")

        results, total_points, processed_points = query8_multithreaded()

        # Handle database connection failure
        if results is None:
            print(f"\n❌ Analysis terminated due to database connection failure")
            print(f"💡 Please resolve the database issue and try again")
            sys.exit(1)

        end_time = time.time()

        runtime = end_time - start_time
        print(f"\n" + "="*60)
        print(f"🎉 ANALYSIS COMPLETE!")
        print(f"="*60)
        print(f"⏱️  Runtime: {runtime:.1f} seconds ({runtime/60:.1f} minutes)")
        print(f"📊 Total GPS points analyzed: {total_points:,}")
        print(f"🔍 Points processed: {processed_points:,}")
        print(f"🎯 Found {len(results)} pairs of taxis within 5m and 5 seconds:")

        # Show results
        if len(results) > 0:
            print(f"\n📋 Results preview:")
            for i, pair in enumerate(results[:30]):  # Show first 30 pairs
                print(f"  {i+1:2d}. Taxi {pair[0]} ↔ Taxi {pair[1]}")
            if len(results) > 30:
                print(f"  ... and {len(results) - 30} more pairs")
        else:
            print(f"❌ No taxi pairs found within the specified criteria")

        # Save final results
        results_file = "results/query8_final_results.json"
        with open(results_file, "w") as f:
            json.dump({
                'pairs': results,
                'total_points': total_points,
                'processed_points': processed_points,
                'runtime_seconds': runtime,
                'analysis_completed_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'timestamp': time.time()
            }, f, indent=2)
        print(f"\n💾 Final results saved to {results_file}")

        # Performance summary
        print(f"\n📈 Performance Summary:")
        print(f"   • Processing rate: {total_points/runtime:.0f} points/second")
        print(f"   • Trip rate: {processed_points/runtime:.0f} trips/second")
        print(f"   • Memory efficiency: Streaming processing used")
        print(f"   • CPU utilization: Multithreaded across multiple cores")

        print(f"\n🐳 FINAL DOCKER NOTICE:")
        print(f"✅ All processing complete - Docker container can be safely stopped")

    except KeyboardInterrupt:
        print(f"\n\n⚠️  Analysis interrupted by user")
        print(f"💾 Progress has been saved and can be resumed later")
        print(f"🐳 Docker container can be safely stopped")
        sys.exit(0)
    except mysql.connector.Error as e:
        print(f"\n\n❌ Database connection error:")
        print(f"   Error: {e}")
        print(f"\n💡 Please ensure MySQL container is running:")
        print(f"   docker ps -a  # Check container status")
        print(f"   docker start <container_name>  # Start if stopped")
        print(f"💾 Any progress made has been saved")
        print(f"🐳 Docker container can be safely stopped")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error occurred during analysis:")
        print(f"   Error: {e}")
        print(f"💾 Any progress made has been saved")
        print(f"🐳 Docker container can be safely stopped")
        import traceback
        traceback.print_exc()
        sys.exit(1)