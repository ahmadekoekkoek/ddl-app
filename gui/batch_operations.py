"""
Batch Operations System
Queue multiple scraping jobs with priority-based processing and session resume.
"""

import os
import json
import time
import uuid
import threading
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict, field
from pathlib import Path
from enum import Enum
from queue import PriorityQueue
from datetime import datetime

from PySide6.QtCore import QObject, Signal, QThread


class JobPriority(Enum):
    """Priority levels for batch jobs."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


class JobStatus(Enum):
    """Status of a batch job."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class BatchJob:
    """Represents a batch scraping job."""
    id: str = ""
    name: str = ""
    priority: int = JobPriority.NORMAL.value
    status: str = JobStatus.PENDING.value

    # Job configuration
    bearer_token: str = ""
    entities: List[str] = field(default_factory=list)
    output_folder: str = ""

    # Progress tracking
    total_items: int = 0
    processed_items: int = 0
    failed_items: int = 0

    # Timing
    created_at: float = 0.0
    started_at: float = 0.0
    completed_at: float = 0.0

    # Resume support
    checkpoint: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    max_retries: int = 3

    # Results
    error_message: str = ""
    result_files: List[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:8]
        if self.created_at == 0.0:
            self.created_at = time.time()
        self.total_items = len(self.entities)

    def __lt__(self, other):
        """Compare jobs by priority for queue ordering."""
        return self.priority < other.priority

    @property
    def progress_percent(self) -> float:
        """Calculate progress percentage."""
        if self.total_items == 0:
            return 0.0
        return (self.processed_items / self.total_items) * 100

    @property
    def elapsed_time(self) -> float:
        """Get elapsed processing time in seconds."""
        if self.started_at == 0:
            return 0.0
        end_time = self.completed_at if self.completed_at > 0 else time.time()
        return end_time - self.started_at

    @property
    def estimated_remaining(self) -> float:
        """Estimate remaining time in seconds."""
        if self.processed_items == 0:
            return 0.0
        rate = self.processed_items / self.elapsed_time
        remaining = self.total_items - self.processed_items
        return remaining / rate if rate > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BatchJob':
        """Create from dictionary."""
        return cls(**data)


class JobWorker(QThread):
    """Worker thread for executing batch jobs."""

    progress = Signal(str, int, int)  # job_id, processed, total
    job_completed = Signal(str, bool, str)  # job_id, success, message
    item_processed = Signal(str, str, bool)  # job_id, item, success

    def __init__(self, job: BatchJob, processor: Callable):
        super().__init__()
        self.job = job
        self.processor = processor
        self._cancelled = False
        self._paused = False
        self._pause_event = threading.Event()
        self._pause_event.set()

    def run(self):
        """Execute the job."""
        self.job.status = JobStatus.RUNNING.value
        self.job.started_at = time.time()

        try:
            # Resume from checkpoint if available
            start_index = self.job.checkpoint.get("last_index", 0)

            for i, entity in enumerate(self.job.entities[start_index:], start=start_index):
                # Check for cancellation
                if self._cancelled:
                    self.job.status = JobStatus.CANCELLED.value
                    self.job_completed.emit(self.job.id, False, "Job cancelled")
                    return

                # Handle pause
                self._pause_event.wait()

                try:
                    # Process the entity
                    success = self.processor(entity, self.job)

                    if success:
                        self.job.processed_items += 1
                    else:
                        self.job.failed_items += 1

                    self.item_processed.emit(self.job.id, entity, success)

                except Exception as e:
                    self.job.failed_items += 1
                    self.item_processed.emit(self.job.id, entity, False)

                # Update checkpoint
                self.job.checkpoint["last_index"] = i + 1
                self.job.checkpoint["timestamp"] = time.time()

                # Emit progress
                self.progress.emit(
                    self.job.id,
                    self.job.processed_items,
                    self.job.total_items
                )

            # Job completed
            self.job.completed_at = time.time()
            self.job.status = JobStatus.COMPLETED.value
            self.job_completed.emit(self.job.id, True, "Job completed successfully")

        except Exception as e:
            self.job.status = JobStatus.FAILED.value
            self.job.error_message = str(e)
            self.job_completed.emit(self.job.id, False, str(e))

    def cancel(self):
        """Cancel the job."""
        self._cancelled = True
        self._pause_event.set()  # Resume if paused to allow exit

    def pause(self):
        """Pause the job."""
        self._paused = True
        self._pause_event.clear()
        self.job.status = JobStatus.PAUSED.value

    def resume(self):
        """Resume the job."""
        self._paused = False
        self._pause_event.set()
        self.job.status = JobStatus.RUNNING.value


class BatchQueue(QObject):
    """Manages the batch job queue."""

    job_added = Signal(BatchJob)
    job_started = Signal(str)
    job_progress = Signal(str, int, int)
    job_completed = Signal(str, bool)
    queue_empty = Signal()

    STATE_FILE = "batch_queue_state.json"

    def __init__(self, max_concurrent: int = 1, state_path: str = None):
        super().__init__()

        self.max_concurrent = max_concurrent
        self.state_path = Path(state_path or ".") / self.STATE_FILE

        self._queue: PriorityQueue = PriorityQueue()
        self._jobs: Dict[str, BatchJob] = {}
        self._workers: Dict[str, JobWorker] = {}
        self._processor: Optional[Callable] = None

        self._running = False
        self._lock = threading.Lock()

        self._load_state()

    def set_processor(self, processor: Callable):
        """Set the job processor function."""
        self._processor = processor

    def add_job(self, job: BatchJob) -> str:
        """Add a job to the queue."""
        with self._lock:
            job.status = JobStatus.QUEUED.value
            self._jobs[job.id] = job
            self._queue.put((job.priority, time.time(), job))

        self._save_state()
        self.job_added.emit(job)

        # Auto-start if running
        if self._running:
            self._process_next()

        return job.id

    def remove_job(self, job_id: str) -> bool:
        """Remove a job from the queue."""
        with self._lock:
            if job_id in self._jobs:
                job = self._jobs[job_id]
                if job.status in [JobStatus.QUEUED.value, JobStatus.PENDING.value]:
                    del self._jobs[job_id]
                    self._save_state()
                    return True
        return False

    def get_job(self, job_id: str) -> Optional[BatchJob]:
        """Get a job by ID."""
        return self._jobs.get(job_id)

    def list_jobs(self, status: JobStatus = None) -> List[BatchJob]:
        """List all jobs, optionally filtered by status."""
        jobs = list(self._jobs.values())
        if status:
            jobs = [j for j in jobs if j.status == status.value]
        return sorted(jobs, key=lambda j: (j.priority, j.created_at))

    def start(self):
        """Start processing the queue."""
        self._running = True
        self._process_next()

    def stop(self):
        """Stop processing the queue."""
        self._running = False

    def pause_job(self, job_id: str):
        """Pause a running job."""
        if job_id in self._workers:
            self._workers[job_id].pause()

    def resume_job(self, job_id: str):
        """Resume a paused job."""
        if job_id in self._workers:
            self._workers[job_id].resume()
        elif job_id in self._jobs:
            # Re-queue the job
            job = self._jobs[job_id]
            if job.status == JobStatus.PAUSED.value:
                job.status = JobStatus.QUEUED.value
                self._queue.put((job.priority, time.time(), job))
                self._process_next()

    def cancel_job(self, job_id: str):
        """Cancel a job."""
        if job_id in self._workers:
            self._workers[job_id].cancel()
        elif job_id in self._jobs:
            self._jobs[job_id].status = JobStatus.CANCELLED.value

        self._save_state()

    def retry_job(self, job_id: str) -> bool:
        """Retry a failed job."""
        if job_id not in self._jobs:
            return False

        job = self._jobs[job_id]
        if job.status != JobStatus.FAILED.value:
            return False

        if job.retry_count >= job.max_retries:
            return False

        job.retry_count += 1
        job.status = JobStatus.QUEUED.value
        job.error_message = ""

        # Resume from last checkpoint
        self._queue.put((job.priority, time.time(), job))

        if self._running:
            self._process_next()

        return True

    def _process_next(self):
        """Process the next job in the queue."""
        if not self._running or not self._processor:
            return

        # Check concurrent limit
        active_workers = sum(1 for w in self._workers.values() if w.isRunning())
        if active_workers >= self.max_concurrent:
            return

        # Get next job
        try:
            _, _, job = self._queue.get_nowait()
        except Exception:
            if not self._workers:
                self.queue_empty.emit()
            return

        # Create and start worker
        worker = JobWorker(job, self._processor)
        worker.progress.connect(self._on_job_progress)
        worker.job_completed.connect(self._on_job_completed)
        worker.finished.connect(lambda: self._on_worker_finished(job.id))

        self._workers[job.id] = worker
        self.job_started.emit(job.id)
        worker.start()

    def _on_job_progress(self, job_id: str, processed: int, total: int):
        """Handle job progress updates."""
        self.job_progress.emit(job_id, processed, total)
        self._save_state()

    def _on_job_completed(self, job_id: str, success: bool, message: str):
        """Handle job completion."""
        self.job_completed.emit(job_id, success)
        self._save_state()

    def _on_worker_finished(self, job_id: str):
        """Handle worker thread completion."""
        if job_id in self._workers:
            del self._workers[job_id]
        self._process_next()

    def _save_state(self):
        """Save queue state for resume capability."""
        state = {
            "jobs": {jid: job.to_dict() for jid, job in self._jobs.items()},
            "saved_at": time.time()
        }

        try:
            with open(self.state_path, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            print(f"Error saving batch queue state: {e}")

    def _load_state(self):
        """Load saved queue state."""
        if not self.state_path.exists():
            return

        try:
            with open(self.state_path, 'r', encoding='utf-8') as f:
                state = json.load(f)

            for job_data in state.get("jobs", {}).values():
                job = BatchJob.from_dict(job_data)

                # Re-queue interrupted jobs
                if job.status in [JobStatus.RUNNING.value, JobStatus.QUEUED.value]:
                    job.status = JobStatus.QUEUED.value
                    self._jobs[job.id] = job
                    self._queue.put((job.priority, job.created_at, job))
                elif job.status not in [JobStatus.COMPLETED.value, JobStatus.CANCELLED.value]:
                    self._jobs[job.id] = job

        except Exception as e:
            print(f"Error loading batch queue state: {e}")

    def get_statistics(self) -> Dict[str, Any]:
        """Get queue statistics."""
        jobs = list(self._jobs.values())

        return {
            "total_jobs": len(jobs),
            "pending": sum(1 for j in jobs if j.status == JobStatus.PENDING.value),
            "queued": sum(1 for j in jobs if j.status == JobStatus.QUEUED.value),
            "running": sum(1 for j in jobs if j.status == JobStatus.RUNNING.value),
            "completed": sum(1 for j in jobs if j.status == JobStatus.COMPLETED.value),
            "failed": sum(1 for j in jobs if j.status == JobStatus.FAILED.value),
            "cancelled": sum(1 for j in jobs if j.status == JobStatus.CANCELLED.value),
            "total_items": sum(j.total_items for j in jobs),
            "processed_items": sum(j.processed_items for j in jobs),
            "failed_items": sum(j.failed_items for j in jobs),
        }

    def clear_completed(self):
        """Remove completed jobs from history."""
        with self._lock:
            completed_ids = [
                jid for jid, job in self._jobs.items()
                if job.status in [JobStatus.COMPLETED.value, JobStatus.CANCELLED.value]
            ]
            for jid in completed_ids:
                del self._jobs[jid]
        self._save_state()


class BatchJobBuilder:
    """Builder pattern for creating batch jobs."""

    def __init__(self, name: str = ""):
        self._job = BatchJob(name=name or f"Batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

    def with_priority(self, priority: JobPriority) -> 'BatchJobBuilder':
        """Set job priority."""
        self._job.priority = priority.value
        return self

    def with_entities(self, entities: List[str]) -> 'BatchJobBuilder':
        """Set entities to process."""
        self._job.entities = entities
        self._job.total_items = len(entities)
        return self

    def with_token(self, token: str) -> 'BatchJobBuilder':
        """Set bearer token."""
        self._job.bearer_token = token
        return self

    def with_output(self, folder: str) -> 'BatchJobBuilder':
        """Set output folder."""
        self._job.output_folder = folder
        return self

    def with_retries(self, max_retries: int) -> 'BatchJobBuilder':
        """Set max retry count."""
        self._job.max_retries = max_retries
        return self

    def build(self) -> BatchJob:
        """Build the job."""
        return self._job


# Singleton instance
_batch_queue: Optional[BatchQueue] = None

def get_batch_queue(max_concurrent: int = 1) -> BatchQueue:
    """Get the global batch queue instance."""
    global _batch_queue
    if _batch_queue is None:
        _batch_queue = BatchQueue(max_concurrent=max_concurrent)
    return _batch_queue
