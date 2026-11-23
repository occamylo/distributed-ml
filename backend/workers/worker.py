import argparse
import ctypes
import json
import logging
import os
import socket
import time
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from uuid import UUID

DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 3.0
REQUEST_TIMEOUT_SECONDS = 10
_MIB = 1024 * 1024
logger = logging.getLogger(__name__)


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
    if os.name == "nt":
        class MemoryStatusEx(ctypes.Structure):
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

        memory_status = MemoryStatusEx()
        memory_status.dwLength = ctypes.sizeof(memory_status)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        global_memory_status = kernel32.GlobalMemoryStatusEx
        global_memory_status.argtypes = [ctypes.POINTER(MemoryStatusEx)]
        global_memory_status.restype = ctypes.c_int
        if not global_memory_status(ctypes.byref(memory_status)):
            return None
        return memory_status.ullTotalPhys // _MIB

    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        page_count = os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, OSError, ValueError):
        return None

    total_bytes = page_size * page_count
    return total_bytes // _MIB if total_bytes > 0 else None


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


def _register_worker(backend_url: str) -> str:
    registration = _post_json(
        f"{backend_url}/nodes",
        {
            "hostname": socket.gethostname(),
            "ip_address": _get_ip_address(),
            "cpu_cores": os.cpu_count(),
            "ram_mb": _get_ram_mb(),
        },
    )
    node_id_value = registration.get("node_id")
    if not isinstance(node_id_value, str):
        raise RuntimeError("Backend registration response did not include a node_id.")
    try:
        return str(UUID(node_id_value))
    except ValueError as exc:
        raise RuntimeError("Backend registration returned an invalid node_id.") from exc


def run_worker(
    backend_url: str,
    heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
) -> None:
    parsed_url = urlparse(backend_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise ValueError("Backend URL must be an absolute HTTP or HTTPS URL.")
    if heartbeat_interval_seconds <= 0:
        raise ValueError("Heartbeat interval must be greater than zero.")

    backend_url = backend_url.rstrip("/")
    node_id = _register_worker(backend_url)
    logger.info("Registered Worker %s as node %s.", socket.gethostname(), node_id)

    try:
        while True:
            _post_json(f"{backend_url}/nodes/{node_id}/heartbeat")
            logger.info("Heartbeat sent for node %s.", node_id)
            time.sleep(heartbeat_interval_seconds)
    except KeyboardInterrupt:
        logger.info("Worker %s stopped.", node_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Register a Worker and send heartbeats.")
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
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    run_worker(args.backend_url, args.heartbeat_interval)


if __name__ == "__main__":
    main()
