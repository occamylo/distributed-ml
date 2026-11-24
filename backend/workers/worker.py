import argparse
import ctypes
import json
import logging
import os
import platform
import socket
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

from psycopg import OperationalError
from psycopg.errors import SerializationFailure
from psycopg_pool import PoolTimeout

from coordinator.scheduler import Scheduler
from database.connection import close_pool, initialize_pool
from database.repository import (
    get_node,
    mark_stale_nodes_offline,
    update_node_metrics,
)
from workers.coordination import WorkerCoordination
from workers.task_executor import TaskExecutor

DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 3.0
REQUEST_TIMEOUT_SECONDS = 10
_MIB = 1024 * 1024
_CPU_SAMPLE_SECONDS = 0.2
logger = logging.getLogger(__name__)


class _MemoryStatusEx(ctypes.Structure):
    _fields_ = [
        ("dwLength", ctypes.c_ulong),
        ("dwMemoryLoad", ctypes.c_ulong),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
    ]


class _FileTime(ctypes.Structure):
    _fields_ = [
        ("dwLowDateTime", ctypes.c_ulong),
        ("dwHighDateTime", ctypes.c_ulong),
    ]


def _get_windows_memory_status() -> tuple[int, int] | None:
    try:
        memory_status = _MemoryStatusEx()
        memory_status.dwLength = ctypes.sizeof(memory_status)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        global_memory_status = kernel32.GlobalMemoryStatusEx
        global_memory_status.argtypes = [ctypes.POINTER(_MemoryStatusEx)]
        global_memory_status.restype = ctypes.c_int
        if not global_memory_status(ctypes.byref(memory_status)):
            return None
    except (AttributeError, OSError):
        return None
    return memory_status.ullTotalPhys, memory_status.ullAvailPhys


def _get_ip_address() -> str | None:
    try:
        addresses = socket.getaddrinfo(
            socket.gethostname(),
            None,
            family=socket.AF_INET,
            type=socket.SOCK_STREAM,
        )
    except OSError:
        return None

    for address in addresses:
        ip_address = address[4][0]
        if not ip_address.startswith("127."):
            return ip_address
    return None


def _get_ram_mb() -> int | None:
    if platform.system() == "Windows":
        memory_status = _get_windows_memory_status()
        return memory_status[0] // _MIB if memory_status is not None else None

    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        page_count = os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        return None

    total_bytes = page_size * page_count
    return total_bytes // _MIB if total_bytes > 0 else None


def _read_linux_cpu_times() -> tuple[int, int] | None:
    try:
        fields = Path("/proc/stat").read_text(encoding="ascii").splitlines()[0].split()
        if fields[0] != "cpu" or len(fields) < 5:
            return None
        counters = [int(value) for value in fields[1:]]
    except (IndexError, OSError, ValueError):
        return None

    idle = counters[3] + (counters[4] if len(counters) > 4 else 0)
    return sum(counters[:8]), idle


def _linux_cpu_capacity_cores(cpu_count: int) -> float:
    try:
        quota_text, period_text = Path("/sys/fs/cgroup/cpu.max").read_text(
            encoding="ascii"
        ).split()
        if quota_text != "max":
            quota, period = int(quota_text), int(period_text)
            if quota > 0 and period > 0:
                return min(float(cpu_count), quota / period)
    except (OSError, ValueError):
        pass

    try:
        quota = int(
            Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").read_text(encoding="ascii")
        )
        period = int(
            Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us").read_text(encoding="ascii")
        )
    except (OSError, ValueError):
        return float(cpu_count)

    if quota > 0 and period > 0:
        return min(float(cpu_count), quota / period)
    return float(cpu_count)


def _get_windows_cpu_times() -> tuple[int, int, int] | None:
    try:
        idle_time = _FileTime()
        kernel_time = _FileTime()
        user_time = _FileTime()
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        get_system_times = kernel32.GetSystemTimes
        get_system_times.argtypes = [
            ctypes.POINTER(_FileTime),
            ctypes.POINTER(_FileTime),
            ctypes.POINTER(_FileTime),
        ]
        get_system_times.restype = ctypes.c_int
        if not get_system_times(
            ctypes.byref(idle_time),
            ctypes.byref(kernel_time),
            ctypes.byref(user_time),
        ):
            return None
    except (AttributeError, OSError):
        return None

    def file_time_value(value: _FileTime) -> int:
        return (value.dwHighDateTime << 32) | value.dwLowDateTime

    return (
        file_time_value(kernel_time),
        file_time_value(user_time),
        file_time_value(idle_time),
    )


def _available_cores_from_sample(
    total_delta: int,
    idle_delta: int,
    capacity_cores: float,
) -> float | None:
    if total_delta <= 0:
        return None
    idle_ratio = max(0.0, min(1.0, idle_delta / total_delta))
    return capacity_cores * idle_ratio


def _get_cpu_available_cores() -> float | None:
    cpu_count = os.cpu_count()
    if cpu_count is None or cpu_count <= 0:
        return None

    if platform.system() == "Windows":
        first = _get_windows_cpu_times()
        if first is None:
            return None
        time.sleep(_CPU_SAMPLE_SECONDS)
        second = _get_windows_cpu_times()
        if second is None:
            return None
        total_delta = (second[0] - first[0]) + (second[1] - first[1])
        idle_delta = second[2] - first[2]
        return _available_cores_from_sample(
            total_delta,
            idle_delta,
            float(cpu_count),
        )

    if platform.system() == "Linux":
        first = _read_linux_cpu_times()
        if first is None:
            return None
        time.sleep(_CPU_SAMPLE_SECONDS)
        second = _read_linux_cpu_times()
        if second is None:
            return None
        return _available_cores_from_sample(
            second[0] - first[0],
            second[1] - first[1],
            _linux_cpu_capacity_cores(cpu_count),
        )

    try:
        load_average = os.getloadavg()[0]
    except (AttributeError, OSError):
        return None
    return max(0.0, min(float(cpu_count), cpu_count - load_average))


def _linux_mem_available_bytes() -> int | None:
    try:
        lines = Path("/proc/meminfo").read_text(encoding="ascii").splitlines()
    except OSError:
        return None
    for line in lines:
        fields = line.split()
        if fields and fields[0] == "MemAvailable:":
            try:
                return int(fields[1]) * 1024
            except (IndexError, ValueError):
                return None
    return None


def _linux_cgroup_mem_available_bytes() -> int | None:
    cgroup_files = (
        (Path("/sys/fs/cgroup/memory.max"), Path("/sys/fs/cgroup/memory.current")),
        (
            Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
            Path("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
        ),
    )
    for limit_path, usage_path in cgroup_files:
        try:
            limit_text = limit_path.read_text(encoding="ascii").strip()
            usage = int(usage_path.read_text(encoding="ascii").strip())
            if limit_text == "max":
                continue
            limit = int(limit_text)
        except (OSError, ValueError):
            continue
        if 0 < limit < 2**60:
            return max(0, limit - usage)
    return None


def _get_ram_available_mb() -> int | None:
    if platform.system() == "Windows":
        memory_status = _get_windows_memory_status()
        return memory_status[1] // _MIB if memory_status is not None else None

    if platform.system() == "Linux":
        available_bytes = _linux_mem_available_bytes()
        if available_bytes is None:
            return None
        cgroup_available = _linux_cgroup_mem_available_bytes()
        if cgroup_available is not None:
            available_bytes = min(available_bytes, cgroup_available)
        return available_bytes // _MIB

    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        available_pages = os.sysconf("SC_AVPHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        return None
    available_bytes = page_size * available_pages
    return available_bytes // _MIB if available_bytes >= 0 else None


def _post_json(url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"

    request = Request(url, data=body, headers=headers, method="POST")
    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            response_body = response.read()
    except HTTPError as exc:
        raise RuntimeError(
            f"Backend returned HTTP {exc.code} for POST {url}."
        ) from exc

    try:
        result = json.loads(response_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Backend returned invalid JSON for POST {url}.") from exc
    if not isinstance(result, dict):
        raise RuntimeError(f"Backend returned a non-object response for POST {url}.")
    return result


def _load_or_create_node_id(node_id_file: Path) -> UUID:
    node_id_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        with node_id_file.open("x", encoding="ascii") as identity_file:
            node_id = uuid4()
            identity_file.write(f"{node_id}\n")
            return node_id
    except FileExistsError:
        pass

    try:
        return UUID(node_id_file.read_text(encoding="ascii").strip())
    except (OSError, UnicodeError, ValueError) as exc:
        raise RuntimeError(
            f"Worker identity file {node_id_file} must contain a valid UUID."
        ) from exc


def _register_worker(
    backend_url: str,
    node_id_file: Path | None = None,
) -> str:
    registration_payload: dict[str, Any] = {
        "hostname": socket.gethostname(),
        "ip_address": _get_ip_address(),
        "cpu_cores": os.cpu_count(),
        "ram_mb": _get_ram_mb(),
    }
    expected_node_id = None
    if node_id_file is not None:
        expected_node_id = _load_or_create_node_id(node_id_file)
        registration_payload["node_id"] = str(expected_node_id)

    registration = _post_json(
        f"{backend_url}/nodes",
        registration_payload,
    )
    node_id_value = registration.get("node_id")
    if not isinstance(node_id_value, str):
        raise RuntimeError("Backend registration response did not include a node_id.")
    try:
        registered_node_id = UUID(node_id_value)
    except ValueError as exc:
        raise RuntimeError("Backend registration returned an invalid node_id.") from exc
    if expected_node_id is not None and registered_node_id != expected_node_id:
        raise RuntimeError(
            "Backend registration returned a node_id different from the persisted "
            "Worker identity."
        )
    return str(registered_node_id)


def run_worker(
    backend_url: str,
    heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    node_id_file: Path | None = None,
) -> None:
    parsed_url = urlparse(backend_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise ValueError("Backend URL must be an absolute HTTP or HTTPS URL.")
    if heartbeat_interval_seconds <= 0:
        raise ValueError("Heartbeat interval must be greater than zero.")

    backend_url = backend_url.rstrip("/")
    node_id = _register_worker(backend_url, node_id_file)
    logger.info("Registered Worker %s as node %s.", socket.gethostname(), node_id)

    pool_initialized = False
    coordination: WorkerCoordination | None = None
    task_executor: TaskExecutor | None = None
    scheduler: Scheduler | None = None
    try:
        while True:
            cycle_started = time.monotonic()
            if not pool_initialized:
                try:
                    initialize_pool()
                    pool_initialized = True
                except (OperationalError, PoolTimeout) as exc:
                    logger.warning(
                        "Worker %s could not connect to CockroachDB (%s); retrying.",
                        node_id,
                        type(exc).__name__,
                    )
                    time.sleep(heartbeat_interval_seconds)
                    continue

            if coordination is None:
                try:
                    node = get_node(UUID(node_id))
                except (OperationalError, PoolTimeout) as exc:
                    logger.warning(
                        "Worker %s could not load its coordination address (%s); "
                        "retrying.",
                        node_id,
                        type(exc).__name__,
                    )
                    time.sleep(heartbeat_interval_seconds)
                    continue
                if node is None:
                    logger.error(
                        "Worker %s is registered, but its CockroachDB node row is missing.",
                        node_id,
                    )
                    time.sleep(heartbeat_interval_seconds)
                    continue
                advertised_host = node["ip_address"] or node["hostname"]
                if not isinstance(advertised_host, str) or not advertised_host:
                    logger.error(
                        "Worker %s has no host address for internal coordination.",
                        node_id,
                    )
                    time.sleep(heartbeat_interval_seconds)
                    continue
                task_executor = TaskExecutor(UUID(node_id))
                coordination = WorkerCoordination(
                    UUID(node_id),
                    advertised_host,
                    task_executor=task_executor,
                )
                coordination.start()
                scheduler = Scheduler(coordination)
                scheduler.start()

            try:
                stale_nodes = mark_stale_nodes_offline()
            except (OperationalError, PoolTimeout, SerializationFailure) as exc:
                logger.warning(
                    "Worker %s could not mark stale nodes offline (%s).",
                    node_id,
                    type(exc).__name__,
                )
            else:
                if stale_nodes:
                    logger.info(
                        "Worker %s marked %s stale node(s) offline.",
                        node_id,
                        len(stale_nodes),
                    )

            try:
                cpu_available_cores = _get_cpu_available_cores()
                ram_available_mb = _get_ram_available_mb()
                updated_node = update_node_metrics(
                    UUID(node_id),
                    cpu_available_cores,
                    ram_available_mb,
                    active_tasks=(
                        task_executor.active_tasks if task_executor is not None else 0
                    ),
                )
            except (OperationalError, PoolTimeout, SerializationFailure) as exc:
                logger.warning(
                    "Worker %s could not publish metrics to CockroachDB (%s); "
                    "retrying in %.1f seconds.",
                    node_id,
                    type(exc).__name__,
                    heartbeat_interval_seconds,
                )
                time.sleep(heartbeat_interval_seconds)
                continue

            if updated_node is None:
                logger.error(
                    "Worker %s is registered, but its CockroachDB node row is missing.",
                    node_id,
                )
            else:
                logger.info(
                    "Published heartbeat and metrics for node %s "
                    "(CPU available: %s cores, RAM available: %s MB).",
                    node_id,
                    cpu_available_cores,
                    ram_available_mb,
                )

            elapsed = time.monotonic() - cycle_started
            time.sleep(max(0.0, heartbeat_interval_seconds - elapsed))
    except KeyboardInterrupt:
        logger.info("Worker %s stopped.", node_id)
    finally:
        if scheduler is not None:
            scheduler.stop()
        if coordination is not None:
            coordination.stop()
        close_pool()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Register a Worker and publish metrics directly to CockroachDB."
    )
    parser.add_argument(
        "--backend-url",
        required=True,
        help="Base URL of the running FastAPI backend, for example http://127.0.0.1:8000",
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        help="Seconds between heartbeats (default: 3).",
    )
    parser.add_argument(
        "--node-id-file",
        type=Path,
        help=(
            "Persistent identity file for this Worker; reuse the same file when "
            "restarting it. Each concurrent Worker needs a distinct file."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    run_worker(args.backend_url, args.heartbeat_interval, args.node_id_file)


if __name__ == "__main__":
    main()
