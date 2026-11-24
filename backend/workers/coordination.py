import json
import logging
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any
from urllib.error import URLError
from urllib.request import Request, urlopen
from uuid import UUID

from psycopg import Error as PsycopgError
from psycopg_pool import PoolTimeout

from database.repository import (
    DEFAULT_CLUSTER_ID,
    DEFAULT_LEASE_DURATION_SECONDS,
    acquire_leadership,
    get_leadership,
    get_node,
    initialize_leadership,
    release_leadership,
    renew_leadership,
    validate_leadership,
)

if TYPE_CHECKING:
    from workers.task_executor import TaskExecutor

DEFAULT_ELECTION_INTERVAL_SECONDS = 3.0
INTERNAL_PORT_BASE = 20000
INTERNAL_PORT_RANGE = 20000
INTERNAL_LEADERSHIP_PATH = "/internal/leadership"
INTERNAL_GRADIENT_PATH = re.compile(
    r"^/internal/tasks/(?P<task_id>[0-9a-fA-F-]{36})/gradient$"
)
MAX_REQUEST_BYTES = 64 * 1024 * 1024
PEER_REQUEST_TIMEOUT_SECONDS = 2.0
logger = logging.getLogger(__name__)


def _port_for_node(node_id: UUID) -> int:
    return INTERNAL_PORT_BASE + node_id.int % INTERNAL_PORT_RANGE


def _host_for_url(host: str) -> str:
    return f"[{host}]" if ":" in host else host


def internal_base_url(node: dict[str, Any]) -> str | None:
    """Base URL of a Worker's internal HTTP service, from its nodes row."""
    host = node.get("ip_address") or node.get("hostname")
    node_id = node.get("node_id")
    if not isinstance(host, str) or not host or not isinstance(node_id, UUID):
        return None
    return f"http://{_host_for_url(host)}:{_port_for_node(node_id)}"


class WorkerCoordination:
    def __init__(
        self,
        node_id: UUID,
        advertised_host: str,
        election_interval_seconds: float = DEFAULT_ELECTION_INTERVAL_SECONDS,
        task_executor: "TaskExecutor | None" = None,
    ) -> None:
        if not advertised_host:
            raise ValueError("An advertised host is required for coordination.")
        if election_interval_seconds <= 0:
            raise ValueError("Election interval must be greater than zero.")

        self.node_id = node_id
        self._task_executor = task_executor
        self._port = _port_for_node(node_id)
        self.url = (
            f"http://{_host_for_url(advertised_host)}:{self._port}"
        )
        self._election_interval_seconds = election_interval_seconds
        self._stop_event = threading.Event()
        self._state_lock = threading.Lock()
        self._is_leader = False
        self._epoch: int | None = None
        self._coordinator_url: str | None = None
        self._server = ThreadingHTTPServer(
            ("0.0.0.0", self._port),
            self._make_request_handler(),
        )
        self._server.daemon_threads = False
        self._http_thread: threading.Thread | None = None
        self._election_thread: threading.Thread | None = None

    @property
    def is_leader(self) -> bool:
        with self._state_lock:
            return self._is_leader

    @property
    def coordinator_url(self) -> str | None:
        with self._state_lock:
            return self._coordinator_url

    def start(self) -> None:
        if self._http_thread is not None:
            raise RuntimeError("The Worker coordination service is already running.")

        self._http_thread = threading.Thread(
            target=self._server.serve_forever,
            name=f"worker-http-{self.node_id}",
            daemon=True,
        )
        self._election_thread = threading.Thread(
            target=self._election_loop,
            name=f"worker-election-{self.node_id}",
            daemon=True,
        )
        self._http_thread.start()
        self._election_thread.start()
        logger.info(
            "Worker %s internal coordination service listening at %s.",
            self.node_id,
            self.url,
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._http_thread is not None:
            self._server.shutdown()
            self._http_thread.join()
        self._server.server_close()
        if self._election_thread is not None:
            self._election_thread.join()
        self._release_leadership_on_stop()

    def _release_leadership_on_stop(self) -> None:
        with self._state_lock:
            was_leader = self._is_leader
            epoch = self._epoch
            self._is_leader = False
            self._epoch = None
            self._coordinator_url = None

        if not was_leader or epoch is None:
            return

        try:
            if not validate_leadership(
                self.node_id,
                epoch,
                DEFAULT_CLUSTER_ID,
            ):
                logger.info(
                    "Worker %s no longer owns leadership at epoch %s; "
                    "the lease will expire normally.",
                    self.node_id,
                    epoch,
                )
                return
            released = release_leadership(
                self.node_id,
                epoch,
                DEFAULT_CLUSTER_ID,
            )
        except (PsycopgError, PoolTimeout) as exc:
            logger.error(
                "Worker %s could not release leadership at epoch %s (%s); "
                "the lease will expire normally.",
                self.node_id,
                epoch,
                type(exc).__name__,
            )
            return

        if not released:
            logger.error(
                "Worker %s could not release leadership at epoch %s; "
                "the lease will expire normally.",
                self.node_id,
                epoch,
            )
            return
        logger.info(
            "Worker %s released leadership at epoch %s.",
            self.node_id,
            epoch,
        )

    def _make_request_handler(self) -> type[BaseHTTPRequestHandler]:
        service = self

        class CoordinationRequestHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path != INTERNAL_LEADERSHIP_PATH:
                    self._send_json(
                        404,
                        {"detail": "Internal coordination route not found."},
                    )
                    return

                try:
                    leadership = get_leadership(DEFAULT_CLUSTER_ID)
                    if leadership is None:
                        self._send_json(
                            503,
                            {"detail": "Leadership record is unavailable."},
                        )
                        return

                    leader_node_id = leadership["leader_node_id"]
                    epoch = leadership["epoch"]
                    is_leader = (
                        leader_node_id == service.node_id
                        and validate_leadership(
                            service.node_id,
                            epoch,
                            DEFAULT_CLUSTER_ID,
                        )
                    )
                    self._send_json(
                        200,
                        {
                            "node_id": str(service.node_id),
                            "leader_node_id": (
                                str(leader_node_id)
                                if leader_node_id is not None
                                else None
                            ),
                            "epoch": epoch,
                            "lease_until": leadership["lease_until"].isoformat(),
                            "is_leader": is_leader,
                        },
                    )
                except (PsycopgError, PoolTimeout) as exc:
                    logger.warning(
                        "Worker %s could not serve leadership status (%s).",
                        service.node_id,
                        type(exc).__name__,
                    )
                    self._send_json(
                        503,
                        {"detail": "Leadership status is temporarily unavailable."},
                    )

            def do_POST(self) -> None:
                match = INTERNAL_GRADIENT_PATH.match(self.path)
                if match is None:
                    self._send_json(404, {"detail": "Internal coordination route not found."})
                    return
                executor = service._task_executor
                if executor is None:
                    self._send_json(503, {"detail": "This Worker does not execute Tasks."})
                    return

                # Imported here to keep the leadership service usable alone.
                from workers.task_executor import GradientRequest, TaskRequestError

                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    if not 0 < length <= MAX_REQUEST_BYTES:
                        raise TaskRequestError(413, "Missing or oversized request body.")
                    payload = json.loads(self.rfile.read(length))
                    request = GradientRequest.parse(UUID(match["task_id"]), payload)
                    response = executor.compute_task_gradient(request)
                except TaskRequestError as exc:
                    self._send_json(exc.status_code, {"detail": exc.detail})
                    return
                except (ValueError, UnicodeDecodeError) as exc:
                    self._send_json(400, {"detail": f"Invalid request ({exc})."})
                    return
                except (PsycopgError, PoolTimeout) as exc:
                    logger.warning(
                        "Worker %s could not check a Task request (%s).",
                        service.node_id,
                        type(exc).__name__,
                    )
                    self._send_json(503, {"detail": "The database is temporarily unavailable."})
                    return
                self._send_json(200, response)

            def _send_json(self, status_code: int, payload: dict[str, Any]) -> None:
                body = json.dumps(payload).encode("utf-8")
                try:
                    self.send_response(status_code)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionError) as exc:
                    # The caller gave up (timeout or shutdown); nothing to answer.
                    logger.debug(
                        "Worker %s could not deliver a response (%s).",
                        service.node_id,
                        type(exc).__name__,
                    )

            def log_message(self, format_string: str, *args: Any) -> None:
                logger.debug(format_string, *args)

        return CoordinationRequestHandler

    def _election_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._refresh_leadership()
            except (PsycopgError, PoolTimeout) as exc:
                self._revoke_authority()
                logger.warning(
                    "Worker %s could not refresh leadership state (%s).",
                    self.node_id,
                    type(exc).__name__,
                )
            self._stop_event.wait(self._election_interval_seconds)

    def _refresh_leadership(self) -> None:
        leadership = get_leadership(DEFAULT_CLUSTER_ID)
        if leadership is None:
            leadership = initialize_leadership(DEFAULT_CLUSTER_ID)

        leader_node_id = leadership["leader_node_id"]
        observed_epoch = leadership["epoch"]

        if leader_node_id == self.node_id:
            renewed = renew_leadership(
                self.node_id,
                observed_epoch,
                DEFAULT_LEASE_DURATION_SECONDS,
                DEFAULT_CLUSTER_ID,
            )
            if renewed is not None:
                self._become_leader(renewed["epoch"])
                return
            self._revoke_authority()

        if not leadership["is_valid"] or leader_node_id == self.node_id:
            self._attempt_acquisition(observed_epoch)
            return

        self._revoke_authority()
        coordinator_url = self._confirm_coordinator(
            leader_node_id,
            observed_epoch,
        )
        self._set_follower(observed_epoch, coordinator_url)

    def _attempt_acquisition(self, observed_epoch: int) -> None:
        acquired = acquire_leadership(
            self.node_id,
            observed_epoch,
            DEFAULT_LEASE_DURATION_SECONDS,
            DEFAULT_CLUSTER_ID,
        )
        if acquired is None:
            self._revoke_authority()
            return
        self._become_leader(acquired["epoch"])

    def _confirm_coordinator(
        self,
        leader_node_id: UUID,
        observed_epoch: int,
    ) -> str | None:
        leader_node = get_node(leader_node_id)
        if leader_node is None:
            logger.error(
                "Leadership references missing Worker node %s.",
                leader_node_id,
            )
            return None

        advertised_host = leader_node["ip_address"] or leader_node["hostname"]
        if not isinstance(advertised_host, str) or not advertised_host:
            logger.error(
                "Worker %s has no advertised host for internal coordination.",
                leader_node_id,
            )
            return None

        coordinator_url = (
            f"http://{_host_for_url(advertised_host)}:"
            f"{_port_for_node(leader_node_id)}{INTERNAL_LEADERSHIP_PATH}"
        )
        request = Request(
            coordinator_url,
            headers={"Accept": "application/json"},
            method="GET",
        )
        try:
            with urlopen(request, timeout=PEER_REQUEST_TIMEOUT_SECONDS) as response:
                payload = json.loads(response.read())
        except (URLError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning(
                "Worker %s could not contact coordinator %s (%s).",
                self.node_id,
                leader_node_id,
                type(exc).__name__,
            )
            return None

        if (
            not isinstance(payload, dict)
            or payload.get("node_id") != str(leader_node_id)
            or payload.get("leader_node_id") != str(leader_node_id)
            or payload.get("epoch") != observed_epoch
            or payload.get("is_leader") is not True
        ):
            logger.warning(
                "Worker %s received an unverified coordinator response from %s.",
                self.node_id,
                leader_node_id,
            )
            return None
        return coordinator_url.removesuffix(INTERNAL_LEADERSHIP_PATH)

    def _become_leader(self, epoch: int) -> None:
        with self._state_lock:
            changed = not self._is_leader or self._epoch != epoch
            self._is_leader = True
            self._epoch = epoch
            self._coordinator_url = self.url
        if changed:
            logger.info(
                "Worker %s acquired leadership at epoch %s.",
                self.node_id,
                epoch,
            )

    def _set_follower(self, epoch: int, coordinator_url: str | None) -> None:
        with self._state_lock:
            was_leader = self._is_leader
            self._is_leader = False
            self._epoch = epoch
            self._coordinator_url = coordinator_url
        if was_leader:
            logger.warning(
                "Worker %s is no longer the coordinator.",
                self.node_id,
            )
        elif coordinator_url is not None:
            logger.debug(
                "Worker %s confirmed coordinator at %s (epoch %s).",
                self.node_id,
                coordinator_url,
                epoch,
            )

    def _revoke_authority(self) -> None:
        with self._state_lock:
            was_leader = self._is_leader
            self._is_leader = False
            self._epoch = None
            self._coordinator_url = None
        if was_leader:
            logger.warning(
                "Worker %s revoked its coordinator authority.",
                self.node_id,
            )
