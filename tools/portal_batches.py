"""Transactional refresh batches and stop fences shared by UI, worker, and CLI."""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta


def now():
    return datetime.now().astimezone().isoformat(timespec="seconds")


def refresh_config():
    def setting(name, default, low, high):
        try:
            return min(high, max(low, int(os.environ.get(name, default))))
        except ValueError:
            return default
    return {"freshness_hours": setting("PORTAL_REFRESH_FRESHNESS_HOURS", 24, 1, 8760),
            "limit": setting("PORTAL_REFRESH_BATCH_SIZE", 10, 1, 100),
            "max_ai_fallbacks": setting("PORTAL_REFRESH_MAX_AI_FALLBACKS", 2, 0, 20)}


class BatchMixin:
    def run_stop_requested(self, run_id):
        with self._connect() as connection:
            row = connection.execute("""SELECT r.state,r.cancel_requested,p.cancel_requested AS parent_stop,p.state AS parent_state
                FROM runs r LEFT JOIN batch_children b ON b.run_id=r.id LEFT JOIN runs p ON p.id=b.batch_id
                WHERE r.id=?""", (run_id,)).fetchone()
            if not row:
                raise self._not_found_error("Run was not found")
            return bool(row["cancel_requested"] or row["state"] not in ("queued", "running") or
                        row["parent_stop"] or row["parent_state"] in ("completed", "cancelled", "interrupted", "failed"))

    def _refresh_options(self, options):
        options = options or {}
        result = refresh_config()
        for name, low, high in (("freshness_hours", 1, 8760), ("limit", 1, 100), ("max_ai_fallbacks", 0, 20)):
            if name in options:
                value = options[name]
                if type(value) is not int or not low <= value <= high:
                    raise self._validation_error(f"{name} must be an integer between {low} and {high}")
                result[name] = value
        result["include_tracked"] = options.get("include_tracked", False)
        if type(result["include_tracked"]) is not bool:
            raise self._validation_error("include_tracked must be boolean")
        return result

    def _eligible_refreshes(self, connection, options):
        cutoff = datetime.now().astimezone() - timedelta(hours=options["freshness_hours"])
        jobs = []
        for row in connection.execute("SELECT * FROM jobs ORDER BY COALESCE(last_checked_at,''), first_seen DESC, id"):
            job = dict(row)
            allowed = job["application_status"] in ("new", "shortlisted") and job["qualification_status"] == "recommended"
            if options["include_tracked"] and job["application_status"] in ("applied", "interviewing"):
                allowed = job["qualification_status"] != "filtered_out"
            if not allowed or job["listing_status"] == "closed" or not job["source_url"]:
                continue
            try:
                checked = datetime.fromisoformat(job["last_checked_at"] or "")
                if checked.tzinfo is None:
                    checked = checked.astimezone()
                if checked > cutoff:
                    continue
            except ValueError:
                pass
            if connection.execute("SELECT 1 FROM runs WHERE job_id=? AND state IN ('queued','running')", (job["id"],)).fetchone():
                continue
            jobs.append({k: job[k] for k in ("id", "company", "title", "application_status", "last_checked_at", "source_url")})
        return jobs

    def preview_refresh_batch(self, options=None):
        options = self._refresh_options(options)
        with self._connect() as connection:
            jobs = self._eligible_refreshes(connection, options)
        return {"options": options, "eligible_count": len(jobs), "jobs": jobs}

    def enqueue_refresh_batch(self, options=None, *, idempotency_key=None):
        raw = options or {}
        options = self._refresh_options(raw)
        ids = raw.get("job_ids")
        if ids is not None and (not isinstance(ids, list) or not ids or len(ids) > 100 or any(not isinstance(i, str) for i in ids)):
            raise self._validation_error("Select one or more stable job IDs (maximum 100)")
        if idempotency_key and (not isinstance(idempotency_key, str) or len(idempotency_key) > 200):
            raise self._validation_error("Invalid idempotency key")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if idempotency_key:
                prior = connection.execute("SELECT r.* FROM run_requests q JOIN runs r ON r.id=q.run_id WHERE q.idempotency_key=?", (idempotency_key,)).fetchone()
                if prior:
                    return self._run_dict(prior)
            # Only one refresh batch at a time: repeated clicks cannot fan out.
            active = connection.execute("SELECT * FROM runs WHERE task_type='refresh_bulk' AND state IN ('queued','running') ORDER BY created_at LIMIT 1").fetchone()
            if active:
                if idempotency_key:
                    connection.execute("INSERT OR IGNORE INTO run_requests VALUES(?,?,?)", (idempotency_key, active["id"], now()))
                connection.commit()
                return self._run_dict(active)
            eligible = self._eligible_refreshes(connection, options)
            if ids is not None:
                selected = set(ids)
                jobs = [j for j in eligible if j["id"] in selected]
                if len(jobs) != len(selected):
                    raise self._conflict_error("Selection changed or contains ineligible jobs; preview again")
            else:
                # Opting into the preview does not implicitly select tracked accounts.
                jobs = [j for j in eligible if j["application_status"] in ("new", "shortlisted")][:options["limit"]]
            if not jobs or len(jobs) > options["limit"]:
                raise self._validation_error("Select eligible jobs within the batch size limit")
            options["job_ids"] = [j["id"] for j in jobs]
            stamp, batch_id = now(), "run_" + uuid.uuid4().hex
            connection.execute("INSERT INTO runs(id,task_type,state,payload,created_at,updated_at) VALUES(?,'refresh_bulk','queued',?,?,?)", (batch_id, json.dumps(options, sort_keys=True), stamp, stamp))
            connection.execute("INSERT INTO run_events(run_id,kind,message,created_at) VALUES(?,'queued',?,?)", (batch_id, f"Batch queued: {len(jobs)} selected listings", stamp))
            for job in jobs:
                child = "run_" + uuid.uuid4().hex
                connection.execute("INSERT INTO runs(id,task_type,job_id,state,payload,created_at,updated_at) VALUES(?,'refresh_listing',?,'queued',?,?,?)", (child, job["id"], json.dumps({"requested_by": batch_id}), stamp, stamp))
                connection.execute("INSERT INTO batch_children VALUES(?,?,?)", (batch_id, child, job["id"]))
                connection.execute("INSERT INTO run_events(run_id,kind,message,created_at) VALUES(?,'queued','Waiting for refresh batch',?)", (child, stamp))
            if idempotency_key:
                connection.execute("INSERT INTO run_requests VALUES(?,?,?)", (idempotency_key, batch_id, stamp))
            connection.commit()
            return self.get_run(batch_id)

    def _check_parent_fence(self, connection, parent_id):
        if not parent_id:
            return
        parent = connection.execute("SELECT * FROM runs WHERE id=?", (parent_id,)).fetchone()
        if parent and (parent["cancel_requested"] or parent["state"] in ("cancelled", "interrupted", "failed") or
                       (parent["task_type"] == "refresh_bulk" and parent["state"] == "completed")):
            raise self._conflict_error("Originating run/batch was stopped; start a new independent request")

    def claim_batch_child(self, batch_id, child_id):
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._check_parent_fence(connection, batch_id)
            parent = connection.execute("SELECT state FROM runs WHERE id=?", (batch_id,)).fetchone()
            if not parent or parent["state"] != "running":
                return None
            child = connection.execute("SELECT r.* FROM runs r JOIN batch_children b ON b.run_id=r.id WHERE b.batch_id=? AND r.id=? AND r.state='queued' AND r.cancel_requested=0", (batch_id, child_id)).fetchone()
            if not child:
                return None
            stamp = now()
            connection.execute("UPDATE runs SET state='running',started_at=?,updated_at=? WHERE id=?", (stamp, stamp, child_id))
            connection.execute("INSERT INTO run_events(run_id,kind,message,created_at) VALUES(?,'running','Checking listing availability',?)", (child_id, stamp))
            connection.commit()
            return self.get_run(child_id)

    def _run_extra(self, connection, result, *, children=True):
        job = connection.execute("SELECT company,title FROM jobs WHERE id=?", (result.get("job_id"),)).fetchone()
        if job:
            result.update({"company": job["company"], "job_title": job["title"]})
        parent = connection.execute("SELECT batch_id FROM batch_children WHERE run_id=?", (result["id"],)).fetchone()
        result["batch_id"] = parent[0] if parent else None
        result["display_state"] = "stopping" if result["cancel_requested"] and result["state"] in ("queued", "running") else result["state"]
        usage = connection.execute("SELECT metrics FROM run_usage WHERE run_id=?", (result["id"],)).fetchone()
        result["usage"] = json.loads(usage[0]) if usage else {}
        result["usage"]["metrics_recorded"] = bool(usage)
        if result["task_type"] == "refresh_listing" and not usage:
            summary = result.get("result_summary", "").replace("’", "'").casefold()
            if summary.startswith(("i'll", "i will", "let me", "i'm going")):
                result["display_summary"] = f"{result['state'].title()} availability task. Legacy summary mixed progress with results; inspect the original events."
        if result["task_type"] == "refresh_bulk" and children:
            rows = connection.execute("SELECT r.* FROM runs r JOIN batch_children b ON b.run_id=r.id WHERE b.batch_id=? ORDER BY r.created_at,r.rowid", (result["id"],)).fetchall()
            result["children"] = [self._run_extra(connection, self._run_dict(row), children=False) for row in rows]
            result["counts"] = {s: sum(c["display_state"] == s for c in result["children"]) for s in ("queued", "running", "stopping", "completed", "failed", "cancelled", "interrupted")}
            result["counts"]["total"] = len(rows)
            result["usage"]["metrics_recorded"] = bool(usage) or any(c["usage"]["metrics_recorded"] for c in result["children"])
            for name in ("deterministic_checks", "cache_hits", "ai_fallbacks", "unresolved"):
                result["usage"][name] = sum(c["usage"].get(name, 0) for c in result["children"])
        result["retry_allowed"] = result["state"] in ("failed", "cancelled", "interrupted") and not (result["task_type"] == "refresh_bulk" and result["cancel_requested"])
        if result["task_type"] == "refresh_bulk" or parent:
            result["retry_allowed"] = False
        if parent:
            p = connection.execute("SELECT cancel_requested FROM runs WHERE id=?", (parent[0],)).fetchone()
            result["retry_allowed"] = result["retry_allowed"] and not p[0]
        return result

    def update_run_usage(self, run_id, **metrics):
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            prior = connection.execute("SELECT metrics FROM run_usage WHERE run_id=?", (run_id,)).fetchone()
            merged = json.loads(prior[0]) if prior else {}
            merged.update(metrics)
            connection.execute("INSERT OR REPLACE INTO run_usage VALUES(?,?)", (run_id, json.dumps(merged)))
            connection.commit()

    def request_stop(self, *, batch_id=None, idempotency_key=None):
        if idempotency_key and (not isinstance(idempotency_key, str) or len(idempotency_key) > 200):
            raise self._validation_error("Invalid idempotency key")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if idempotency_key:
                prior = connection.execute("SELECT id FROM run_stops WHERE request_key=?", (idempotency_key,)).fetchone()
                if prior:
                    return self.get_stop(prior[0])
            if batch_id:
                parent = connection.execute("SELECT * FROM runs WHERE id=? AND task_type='refresh_bulk'", (batch_id,)).fetchone()
                if not parent:
                    raise self._not_found_error("Refresh batch not found")
                rows = connection.execute("SELECT * FROM runs WHERE id=? OR id IN (SELECT run_id FROM batch_children WHERE batch_id=?)", (batch_id, batch_id)).fetchall()
            else:
                rows = connection.execute("SELECT * FROM runs WHERE state IN ('queued','running')").fetchall()
                connection.execute("UPDATE runs SET cancel_requested=1 WHERE id IN (SELECT b.batch_id FROM batch_children b JOIN runs r ON r.id=b.run_id WHERE r.state IN ('queued','running'))")
            stamp, queued, active = now(), [], []
            for row in rows:
                # Fence terminal parents too: no later children/retries may escape a stop.
                if row["task_type"] == "refresh_bulk":
                    connection.execute("UPDATE runs SET cancel_requested=1,updated_at=? WHERE id=?", (stamp, row["id"]))
                if row["state"] == "queued":
                    queued.append(row["id"])
                    connection.execute("UPDATE runs SET state='cancelled',cancel_requested=1,result_summary='Cancelled before starting',ended_at=?,updated_at=? WHERE id=?", (stamp, stamp, row["id"]))
                elif row["state"] == "running":
                    active.append(row["id"])
                    connection.execute("UPDATE runs SET cancel_requested=1,updated_at=? WHERE id=?", (stamp, row["id"]))
                else:
                    continue
                connection.execute("INSERT INTO run_events(run_id,kind,message,created_at) VALUES(?,?,?,?)", (row["id"], "cancelled" if row["state"] == "queued" else "stopping", "Cancelled before starting" if row["state"] == "queued" else "Stopping requested", stamp))
            stop_id = "stop_" + uuid.uuid4().hex
            data = {"queued_ids": queued, "active_ids": active, "failures": []}
            connection.execute("INSERT INTO run_stops VALUES(?,?,?,?,?)", (stop_id, idempotency_key, batch_id, json.dumps(data), stamp))
            connection.commit()
            return self.get_stop(stop_id)

    def get_stop(self, stop_id):
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM run_stops WHERE id=?", (stop_id,)).fetchone()
            if not row:
                raise self._not_found_error("Stop request not found")
            data = json.loads(row["data"])
            states = {r["id"]: r["state"] for r in connection.execute("SELECT id,state FROM runs")}
            data.update({"id": stop_id, "batch_id": row["batch_id"], "cancelled_queued": len(data["queued_ids"]),
                         "interrupted_active": sum(states.get(i) in ("cancelled", "interrupted") for i in data["active_ids"]),
                         "stopping": sum(states.get(i) in ("queued", "running") for i in data["active_ids"]),
                         "finished_before_stop": sum(states.get(i) == "completed" for i in data["active_ids"])})
            data["state"] = "stopping" if data["stopping"] else "completed"
            return data

    def stop_failure(self, stop_id, run_id, error):
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT data FROM run_stops WHERE id=?", (stop_id,)).fetchone()
            data = json.loads(row[0])
            data["failures"].append({"run_id": run_id, "error": str(error)[:500]})
            connection.execute("UPDATE run_stops SET data=? WHERE id=?", (json.dumps(data), stop_id))
            connection.commit()
