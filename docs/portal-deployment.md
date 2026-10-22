# Local job-search portal

Run `/setup` in your own private working copy before using personalized search or CV generation. The public template contains no imported applications, candidate documents, login tokens or runtime database.

## Requirements and startup

The portal targets Linux/macOS: state locking uses Python's `fcntl` module. Use Python 3.10 or newer, the dependencies in `deploy/portal-requirements.txt`, an authenticated agent CLI for agent-backed tasks, Bun for search CLIs, and the LaTeX/PDF tools described in SETUP.md.

```sh
python3 tools/cv_webview.py --repo . --host 127.0.0.1 --port 8765
```

Open `http://127.0.0.1:8765` on the same machine. Keep the listener on loopback. The portal holds private candidate and application data after setup and must not be exposed as an unauthenticated website.

To reach a server's loopback listener through your own SSH connection:

```sh
ssh -N -L 8765:127.0.0.1:8765 jobsearch@SERVER
```

The service and environment examples under `deploy/` use an example `jobsearch` account. Adjust paths and permissions before enabling them. Do not copy another user's agent credentials or personal profile into a shared deployment.

## Runtime state

The portal coordinates jobs, statuses, artifacts, queues and conversations in `portal_data/`. Legacy inputs are the private `job_search_tracker.csv` and `job_scraper/seen_jobs.json` files. They are absent from this template; `job_search_tracker.example.csv` documents the CSV columns.

Runtime state, generated applications, agent sessions and tracker contents belong in your private copy. Migration can create backups; keep those private too. Back up state before upgrading, stop the service while restoring, and verify the restored state before resuming tasks.

## CVs and concurrency

CV generation is explicit and tied to a selected job; it does not change application status. Validation checks the PDF page count, text length and configured candidate name. Blank profile placeholders are not used as a person's name.

`PORTAL_CV_CONCURRENCY` controls bounded parallel workers. The default is two; four is supported when the machine and agent account can support that workload. Worker concurrency is separate from account limits. Availability refresh uses a separate HTTP-first queue: timeout, blocking or a login wall means unknown availability, not a closed listing. Preview and select bulk refreshes before starting them.

## Remote access

The Cloudflare ingress example uses `jobs.example.com`. Configure and verify access controls before using a reverse proxy; a hostname or tunnel alone is not authentication. Keep the origin private and confirm unauthenticated requests are denied before using real candidate data.

## Checks

```sh
python3 tools/lint_skills.py
python3 tools/security_guards.py
python3 tools/check_public_template.py
python3 -m unittest discover -s tests -t . -v
```

The public-template check deliberately fails after personalization. Use it to review template copies for publication; keep your application workspace private.
