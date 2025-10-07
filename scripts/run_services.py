#!/usr/bin/env python3
"""
Single-server deployment script for PII Masking API with Celery workers
This script runs both FastAPI server and Celery workers on the same machine
"""

import os
import sys
import signal
import subprocess
import time
import logging
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

class ServiceManager:
    def __init__(self):
        self.processes = []
        self.redis_process = None
        self.celery_process = None
        self.api_process = None

    def check_redis_running(self):
        """Check if Redis is already running"""
        redis_commands = ['redis-cli', 'memurai-cli']

        for cmd in redis_commands:
            try:
                result = subprocess.run([cmd, 'ping'],
                                      capture_output=True, text=True, timeout=5)
                if result.returncode == 0 and 'PONG' in result.stdout:
                    logger.info(f"Redis is already running (via {cmd})")
                    return True
            except (subprocess.TimeoutExpired, FileNotFoundError):
                continue

        # Try connecting via Python redis client
        try:
            import redis
            r = redis.Redis(host='localhost', port=6379, db=0, socket_timeout=2)
            r.ping()
            logger.info("Redis is running (detected via Python client)")
            return True
        except:
            pass

        return False

    def find_redis_executable(self):
        """Find Redis executable on the system"""
        possible_paths = [
            'redis-server',  # In PATH
            'memurai',       # Memurai alternative
            r'C:\Program Files\Redis\redis-server.exe',
            r'C:\Program Files (x86)\Redis\redis-server.exe',
            r'C:\Redis\redis-server.exe',
            r'C:\tools\redis\redis-server.exe',
        ]

        for path in possible_paths:
            try:
                result = subprocess.run([path, '--version'],
                                      capture_output=True, timeout=3)
                if result.returncode == 0:
                    logger.info(f"Found Redis at: {path}")
                    return path
            except:
                continue

        return None

    def start_redis(self):
        """Start Redis server if not already running"""
        # First check if Redis is already running
        if self.check_redis_running():
            return True

        logger.info("Redis not running, attempting to start...")

        # Find Redis executable
        redis_exe = self.find_redis_executable()
        if not redis_exe:
            logger.error("❌ Redis server not found!")
            logger.error("")
            logger.error("📋 Installation Options:")
            logger.error("1. Redis for Windows:")
            logger.error("   - Download: https://github.com/tporadowski/redis/releases")
            logger.error("   - Install Redis-x64-5.0.14.1.msi")
            logger.error("")
            logger.error("2. Memurai (Redis-compatible):")
            logger.error("   - Download: https://www.memurai.com/get-memurai")
            logger.error("")
            logger.error("3. Docker:")
            logger.error("   - docker run -d -p 6379:6379 redis")
            logger.error("")
            logger.error("📖 See REDIS_SETUP.md for detailed instructions")
            logger.error("")
            logger.error("🔧 Quick Test:")
            logger.error("   python scripts/test_redis.py")
            return False

        # Try to start Redis
        try:
            logger.info(f"Starting Redis using: {redis_exe}")

            if os.name == 'nt':  # Windows
                # Start Redis without console window
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = subprocess.SW_HIDE

                self.redis_process = subprocess.Popen(
                    [redis_exe],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    startupinfo=startupinfo
                )
            else:  # Linux/Mac
                self.redis_process = subprocess.Popen(
                    [redis_exe, '--daemonize', 'yes'],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )

            self.processes.append(self.redis_process)

            # Wait and verify Redis started
            logger.info("⏳ Waiting for Redis to start...")
            for i in range(10):  # Try for 10 seconds
                time.sleep(1)
                if self.check_redis_running():
                    logger.info("✅ Redis started successfully")
                    return True

            logger.error("❌ Redis started but not responding")
            logger.error("💡 Try running manually: redis-server")
            return False

        except Exception as e:
            logger.error(f"❌ Failed to start Redis: {e}")
            logger.error("💡 Try starting Redis manually or check REDIS_SETUP.md")
            return False

    def start_celery_worker(self):
        """Start Celery worker"""
        logger.info("Starting Celery worker...")
        try:
            # Change to project directory
            os.chdir(project_root)

            # Start Celery worker
            celery_cmd = [
                sys.executable, '-m', 'celery',
                '-A', 'pii_masking.core.celery_app',
                'worker',
                '--loglevel=info',
                '--concurrency=2',
                '--pool=threads' if os.name == 'nt' else '--pool=prefork'
            ]

            self.celery_process = subprocess.Popen(
                celery_cmd,
                env=dict(os.environ, PYTHONPATH=str(project_root))
            )

            self.processes.append(self.celery_process)
            logger.info(f"Celery worker started with PID: {self.celery_process.pid}")
            return True

        except Exception as e:
            logger.error(f"Failed to start Celery worker: {e}")
            return False

    def start_api_server(self):
        """Start FastAPI server"""
        logger.info("Starting FastAPI server...")
        try:
            # Change to project directory
            os.chdir(project_root)

            # Start FastAPI server
            api_cmd = [
                sys.executable, '-m', 'uvicorn',
                'pii_masking.main:app',
                '--host', '0.0.0.0',
                '--port', '8000',
                '--reload'
            ]

            self.api_process = subprocess.Popen(
                api_cmd,
                env=dict(os.environ, PYTHONPATH=str(project_root))
            )

            self.processes.append(self.api_process)
            logger.info(f"FastAPI server started with PID: {self.api_process.pid}")
            logger.info("API available at: http://localhost:8000")
            logger.info("API docs available at: http://localhost:8000/docs")
            return True

        except Exception as e:
            logger.error(f"Failed to start FastAPI server: {e}")
            return False

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down...")
            self.shutdown()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def shutdown(self):
        """Gracefully shutdown all processes"""
        logger.info("Shutting down services...")

        # Terminate processes in reverse order
        for process in reversed(self.processes):
            if process and process.poll() is None:
                try:
                    logger.info(f"Terminating process {process.pid}")
                    process.terminate()

                    # Wait for graceful shutdown
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        logger.warning(f"Force killing process {process.pid}")
                        process.kill()

                except Exception as e:
                    logger.error(f"Error terminating process: {e}")

        logger.info("All services shut down")

    def wait_for_processes(self):
        """Wait for processes and handle failures"""
        try:
            while True:
                time.sleep(1)

                # Check if any critical process has died
                if self.celery_process and self.celery_process.poll() is not None:
                    logger.error("Celery worker died, restarting...")
                    self.start_celery_worker()

                if self.api_process and self.api_process.poll() is not None:
                    logger.error("API server died, restarting...")
                    self.start_api_server()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
            self.shutdown()

    def run(self):
        """Main run method"""
        logger.info("Starting PII Masking API with Celery workers...")

        # Setup signal handlers
        self.setup_signal_handlers()

        # Check environment
        env_file = project_root / ".env"
        if not env_file.exists():
            logger.warning(f".env file not found at {env_file}")
            logger.warning("Make sure to configure your environment variables")

        # Start services in order
        if not self.start_redis():
            logger.error("Failed to start Redis, exiting...")
            return False

        time.sleep(2)  # Give Redis time to fully start

        if not self.start_celery_worker():
            logger.error("Failed to start Celery worker, exiting...")
            self.shutdown()
            return False

        time.sleep(2)  # Give Celery time to start

        if not self.start_api_server():
            logger.error("Failed to start API server, exiting...")
            self.shutdown()
            return False

        logger.info("All services started successfully!")
        logger.info("Press Ctrl+C to stop all services")

        # Wait and monitor processes
        self.wait_for_processes()

        return True


def main():
    """Main function"""
    # Check Python version
    if sys.version_info < (3, 8):
        logger.error("Python 3.8 or higher is required")
        return 1

    # Check if we're in a virtual environment (recommended)
    if not hasattr(sys, 'base_prefix') or sys.base_prefix == sys.prefix:
        logger.warning("Not running in a virtual environment (recommended to use venv)")

    # Create service manager and run
    manager = ServiceManager()
    success = manager.run()

    return 0 if success else 1


if __name__ == '__main__':
    exit(main())