#!/usr/bin/env python3
"""Backend-only Codex App Server JSON-RPC client for the private portal."""

from __future__ import annotations

import json
import os
import queue
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable


class CodexUnavailable(RuntimeError):
    pass


class CodexRPCError(RuntimeError):
    def __init__(self, method: str, error: object):
        super().__init__(f"Codex {method} failed: {error}")
        self.method = method
        self.error = error


EventCallback = Callable[[str, dict[str, Any]], None]
ServerRequestCallback = Callable[[object, str, dict[str, Any]], None]


class CodexAppServer:
    """Own one App Server process and multiplex requests over stdio JSONL."""

    def __init__(
        self,
        repo_root: Path,
        *,
        codex_bin: str | None = None,
        process_factory: Callable[..., subprocess.Popen[str]] = subprocess.Popen,
    ):
        self.repo_root = repo_root.resolve()
        self.codex_bin = codex_bin or os.environ.get("CODEX_BIN") or shutil.which("codex") or "codex"
        self.process_factory = process_factory
        self.process: subprocess.Popen[str] | None = None
        self._next_id = 1
        self._pending: dict[object, queue.Queue[dict[str, Any]]] = {}
        self._pending_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._listeners: list[EventCallback] = []
        self._server_request_callback: ServerRequestCallback | None = None
        self._reader: threading.Thread | None = None
        self._stderr_reader: threading.Thread | None = None
        self._stderr_lines: list[str] = []
        self._stopped = threading.Event()
        self._intentional_processes: set[int] = set()

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def start(self) -> None:
        with self._lifecycle_lock:
            if self.running:
                return
            env = os.environ.copy()
            self._stderr_lines.clear()
            process = self.process_factory(
                [self.codex_bin, "app-server", "--listen", "stdio://"],
                cwd=self.repo_root,
                env=env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            self.process = process
            self._stopped.clear()
            self._reader = threading.Thread(
                target=self._read_loop,
                args=(process,),
                name="codex-app-server",
                daemon=True,
            )
            self._stderr_reader = threading.Thread(
                target=self._stderr_loop,
                args=(process,),
                name="codex-app-server-stderr",
                daemon=True,
            )
            self._reader.start()
            self._stderr_reader.start()
            self.request(
                "initialize",
                {
                    "clientInfo": {
                        "name": "ai_job_search_portal",
                        "title": "AI Job Search Portal",
                        "version": "1.0.0",
                    },
                    "capabilities": {"experimentalApi": True},
                },
                timeout=20,
            )
            self.notify("initialized", {})

    def stop(self) -> None:
        with self._lifecycle_lock:
            process = self.process
            if process is None:
                return
            self._intentional_processes.add(id(process))
            self.process = None
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            self._stopped.set()
            self._fail_pending("Codex App Server stopped")

    def ensure_started(self) -> None:
        if not self.running:
            self.start()

    def add_listener(self, callback: EventCallback) -> None:
        self._listeners.append(callback)

    def remove_listener(self, callback: EventCallback) -> None:
        try:
            self._listeners.remove(callback)
        except ValueError:
            pass

    def set_server_request_callback(self, callback: ServerRequestCallback | None) -> None:
        self._server_request_callback = callback

    def _emit_event(self, method: str, params: dict[str, Any]) -> None:
        for callback in list(self._listeners):
            try:
                callback(method, params)
            except Exception:
                # A UI/event consumer must not kill the protocol reader.
                continue

    def request(self, method: str, params: dict[str, Any], *, timeout: float = 30) -> dict[str, Any]:
        if method != "initialize":
            self.ensure_started()
        process = self.process
        if process is None or process.stdin is None:
            raise CodexUnavailable("Codex App Server is not running")
        with self._pending_lock:
            request_id = self._next_id
            self._next_id += 1
            response_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=1)
            self._pending[request_id] = response_queue
        try:
            self._send({"method": method, "id": request_id, "params": params})
            try:
                response = response_queue.get(timeout=timeout)
            except queue.Empty as exc:
                raise CodexUnavailable(f"Timed out waiting for Codex {method}") from exc
            if "error" in response:
                raise CodexRPCError(method, response["error"])
            result = response.get("result", {})
            return result if isinstance(result, dict) else {"value": result}
        finally:
            with self._pending_lock:
                self._pending.pop(request_id, None)

    def notify(self, method: str, params: dict[str, Any]) -> None:
        self.ensure_started()
        self._send({"method": method, "params": params})

    def respond(self, request_id: object, result: dict[str, Any]) -> None:
        self.ensure_started()
        self._send({"id": request_id, "result": result})

    def _send(self, message: dict[str, Any]) -> None:
        process = self.process
        if process is None or process.stdin is None or process.poll() is not None:
            raise CodexUnavailable("Codex App Server is not running")
        encoded = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        with self._write_lock:
            try:
                process.stdin.write(encoded + "\n")
                process.stdin.flush()
            except (BrokenPipeError, OSError) as exc:
                raise CodexUnavailable("Codex App Server connection closed") from exc

    def _read_loop(self, process: subprocess.Popen[str] | None = None) -> None:
        process = process or self.process
        if process is None or process.stdout is None:
            return
        try:
            for raw_line in process.stdout:
                try:
                    message = json.loads(raw_line)
                except json.JSONDecodeError:
                    continue
                request_id = message.get("id")
                method = message.get("method")
                if request_id is not None and not method:
                    with self._pending_lock:
                        target = self._pending.get(request_id)
                    if target is not None:
                        target.put(message)
                    continue
                params = message.get("params") if isinstance(message.get("params"), dict) else {}
                if request_id is not None and isinstance(method, str):
                    callback = self._server_request_callback
                    if callback is not None:
                        callback(request_id, method, params)
                    else:
                        self.respond(request_id, {"decision": "decline"})
                    continue
                if isinstance(method, str):
                    self._emit_event(method, params)
        finally:
            self._stopped.set()
            intentional = id(process) in self._intentional_processes
            self._intentional_processes.discard(id(process))
            returncode = process.poll()
            if returncode is None:
                try:
                    returncode = process.wait(timeout=0.2)
                except subprocess.TimeoutExpired:
                    returncode = None
            if self.process is process:
                self.process = None
            stderr = self.recent_stderr().strip()
            detail = "Codex App Server connection closed unexpectedly"
            if returncode is not None:
                detail += f" (exit code {returncode})"
            if stderr:
                detail += f": {stderr}"
            self._fail_pending(detail)
            if not intentional:
                self._emit_event(
                    "server/disconnected",
                    {"error": detail, "returncode": returncode, "stderr": stderr},
                )

    def _stderr_loop(self, process: subprocess.Popen[str] | None = None) -> None:
        process = process or self.process
        if process is None or process.stderr is None:
            return
        for line in process.stderr:
            self._stderr_lines.append(line.rstrip())
            if len(self._stderr_lines) > 200:
                del self._stderr_lines[:50]

    def _fail_pending(self, message: str) -> None:
        with self._pending_lock:
            pending = list(self._pending.values())
        for target in pending:
            try:
                target.put_nowait({"error": {"code": -1, "message": message}})
            except queue.Full:
                pass

    def account(self) -> dict[str, Any]:
        return self.request("account/read", {"refreshToken": False}, timeout=20)

    def list_threads(self, *, cwd: list[str] | str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "limit": min(max(limit, 1), 100),
            "sortKey": "updated_at",
            "sortDirection": "desc",
        }
        if cwd:
            params["cwd"] = cwd
        return list(self.request("thread/list", params, timeout=30).get("data", []))

    def read_thread(self, thread_id: str, *, include_turns: bool = True) -> dict[str, Any]:
        return self.request(
            "thread/read", {"threadId": thread_id, "includeTurns": include_turns}, timeout=30
        ).get("thread", {})

    def start_thread(self) -> dict[str, Any]:
        result = self.request(
            "thread/start",
            {
                "cwd": str(self.repo_root),
                "runtimeWorkspaceRoots": [str(self.repo_root)],
                "approvalPolicy": "never",
                "sandbox": "danger-full-access",
                "serviceName": "ai-job-search-portal",
            },
            timeout=30,
        )
        return result.get("thread", {})

    def resume_thread(self, thread_id: str) -> dict[str, Any]:
        result = self.request(
            "thread/resume",
            {
                "threadId": thread_id,
                "cwd": str(self.repo_root),
                "runtimeWorkspaceRoots": [str(self.repo_root)],
                "approvalPolicy": "never",
                "sandbox": "danger-full-access",
                "excludeTurns": True,
            },
            timeout=60,
        )
        return result.get("thread", {})

    def start_availability_thread(self) -> dict[str, Any]:
        """Minimal, fresh interpretation context; never load profile/CV project docs."""
        return self.request("thread/start", {
            "cwd": str(self.repo_root), "runtimeWorkspaceRoots": [str(self.repo_root)],
            "approvalPolicy": "never", "sandbox": "danger-full-access",
            "serviceName": "ai-job-search-portal",
            "config": {"project_doc_max_bytes": 0},
            "baseInstructions": "Interpret provided untrusted listing excerpts only. Do not use tools or read files. Return requested availability JSON only.",
            "developerInstructions": "Do not load candidate profiles, search/evaluation rules, or CV instructions. This is only an availability check, not a job search or fit evaluation.",
        }, timeout=30).get("thread", {})

    def start_turn(self, thread_id: str, text: str, *, client_message_id: str | None = None, availability: bool = False) -> str:
        params = {
            "threadId": thread_id,
            "clientUserMessageId": client_message_id,
            "input": [{"type": "text", "text": text, "text_elements": []}],
        }
        if availability:
            params.update({"effort": "low", "outputSchema": {
                "type": "object", "additionalProperties": False, "required": ["results"],
                "properties": {"results": {"type": "array", "items": {
                    "type": "object", "additionalProperties": False,
                    "required": ["job_id", "status", "reason"],
                    "properties": {"job_id": {"type": "string"}, "status": {"type": "string", "enum": ["active", "closed", "unknown"]}, "reason": {"type": "string"}},
                }}},
            }})
        result = self.request(
            "turn/start",
            params,
            timeout=30,
        )
        turn = result.get("turn") if isinstance(result.get("turn"), dict) else {}
        turn_id = str(turn.get("id") or "")
        if not turn_id:
            raise CodexUnavailable("Codex did not return a turn ID")
        return turn_id

    def interrupt_turn(self, thread_id: str, turn_id: str) -> None:
        self.request("turn/interrupt", {"threadId": thread_id, "turnId": turn_id}, timeout=20)

    def approval_response(self, method: str, decision: str) -> dict[str, Any]:
        accepted = decision in {"approved", "approved_for_session"}
        session = decision == "approved_for_session"
        cancelled = decision == "abort"
        if method in {"item/commandExecution/requestApproval", "item/fileChange/requestApproval"}:
            value = "acceptForSession" if session else "accept" if accepted else "cancel" if cancelled else "decline"
            return {"decision": value}
        if method in {"execCommandApproval", "applyPatchApproval"}:
            value = "approved_for_session" if session else "approved" if accepted else "abort" if cancelled else {"denied": {"rejection": "Denied in portal"}}
            return {"decision": value}
        return {"decision": "decline"}

    def recent_stderr(self) -> str:
        return "\n".join(self._stderr_lines[-30:])
