#!/usr/bin/env python3
"""
Master orchestration script for HargaPangan Big Data pipeline.

Runs all components:
1. API Producer (fetches data from BI API)
2. RSS Producer (fetches RSS feeds)
3. HDFS Consumer (writes to HDFS)
4. Spark Scheduler (analyzes data every 10 mins)
5. Dashboard (Flask app for visualization)
"""

import os
import sys
import subprocess
import signal
import logging
import time
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("Orchestrator")

# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent
KAFKA_DIR = PROJECT_ROOT / "kafka"
SPARK_DIR = PROJECT_ROOT / "spark"
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"

# Process management
processes = {}

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    log.info("Received interrupt signal, shutting down...")
    shutdown_all()
    sys.exit(0)

def run_process(name, script_path, cwd=None):
    """Run a Python script as a subprocess."""
    if not script_path.exists():
        log.error(f"Script not found: {script_path}")
        return None
    
    try:
        # Determine working directory
        work_dir = cwd or script_path.parent
        
        log.info(f"Starting {name}...")
        process = subprocess.Popen(
            [sys.executable, str(script_path)],
            cwd=str(work_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1
        )
        processes[name] = process
        log.info(f"{name} started with PID {process.pid}")
        return process
    except Exception as e:
        log.error(f"Failed to start {name}: {e}")
        return None

def shutdown_all():
    """Shutdown all running processes."""
    log.info("Shutting down all processes...")
    for name, process in processes.items():
        if process and process.poll() is None:
            log.info(f"Terminating {name} (PID {process.pid})...")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log.warning(f"Force killing {name}...")
                process.kill()

def check_kafka():
    """Check if Kafka is running."""
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('localhost', 9092))
        sock.close()
        return result == 0
    except:
        return False

def check_hdfs():
    """Check if HDFS is running."""
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('localhost', 9000))
        sock.close()
        return result == 0
    except:
        return False

def main():
    """Main orchestration logic."""
    log.info("="*60)
    log.info("HargaPangan Big Data Pipeline Orchestrator")
    log.info("="*60)
    
    # Register signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    # Check infrastructure
    log.info("Checking infrastructure...")
    if not check_kafka():
        log.error("Kafka is not running! Start it with:")
        log.error("  docker-compose -f docker-compose-kafka.yml up -d")
        sys.exit(1)
    log.info("✓ Kafka is running")
    
    if not check_hdfs():
        log.error("HDFS is not running! Start it with:")
        log.error("  docker-compose -f docker-compose-hadoop.yml up -d")
        sys.exit(1)
    log.info("✓ HDFS is running")
    
    # Start all components
    log.info("Starting all components...")
    
    components = [
        ("API Producer", KAFKA_DIR / "producer_api.py"),
        ("RSS Producer", KAFKA_DIR / "producer_rss.py"),
        ("HDFS Consumer", KAFKA_DIR / "consumer_to_hdfs_updated.py"),
        ("Spark Scheduler", SPARK_DIR / "spark_scheduler.py"),
        ("Dashboard", DASHBOARD_DIR / "app.py"),
    ]
    
    started = []
    for name, script in components:
        if run_process(name, script):
            started.append(name)
            time.sleep(2)  # Stagger starts
    
    if not started:
        log.error("Failed to start any components!")
        sys.exit(1)
    
    log.info("="*60)
    log.info(f"Successfully started {len(started)} components:")
    for name in started:
        log.info(f"  ✓ {name}")
    log.info("="*60)
    log.info("Dashboard available at: http://0.0.0.0:5000 (or http://localhost:5000)")
    log.info("Press Ctrl+C to stop all services")
    log.info("="*60)
    
    # Monitor processes
    try:
        while True:
            for name, process in list(processes.items()):
                if process.poll() is not None:
                    log.warning(f"{name} has exited with code {process.returncode}")
            time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        shutdown_all()

if __name__ == "__main__":
    main()
