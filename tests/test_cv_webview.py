"""Tests for the private CV browser viewer."""

from __future__ import annotations

import csv
import io
import json
import tempfile
import threading
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, quote, urlsplit
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from tools.cv_webview import (
    create_server,
    discover_application_links,
    discover_documents,
    is_loopback_host,
    mark_suggested_job_applied,
    resolve_document_path,
    save_uploaded_cv,
)


PDF_BYTES = b"%PDF-1.4\n% test fixture\n%%EOF\n"


def make_docx_bytes() -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("[Content_Types].xml", "<Types />")
        archive.writestr("word/document.xml", "<document><body><p>CV</p></body></document>")
    return output.getvalue()


class CVWebViewDiscoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo = Path(self.temp_dir.name)
        (self.repo / "cv").mkdir()
        (self.repo / "cover_letters").mkdir()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_discovers_only_pdf_files_in_allowed_directories(self) -> None:
        (self.repo / "cv" / "Candidate_Master_CV.pdf").write_bytes(PDF_BYTES)
        (self.repo / "cv" / "External_AI_CV.docx").write_bytes(make_docx_bytes())
        (self.repo / "cv" / "Candidate_Master_CV.tex").write_text("ignored", encoding="utf-8")
        (self.repo / "cover_letters" / "cover_acme_role.pdf").write_bytes(PDF_BYTES)

        documents = discover_documents(self.repo)

        self.assertEqual(
            {doc["id"] for doc in documents},
            {
                "cv/Candidate_Master_CV.pdf",
                "cv/External_AI_CV.docx",
                "cover_letters/cover_acme_role.pdf",
            },
        )
        master = next(doc for doc in documents if doc["id"] == "cv/Candidate_Master_CV.pdf")
        self.assertEqual(master["title"], "Master CV")
        self.assertEqual(master["kind"], "cv")
        self.assertEqual(master["format"], "PDF")
        self.assertEqual(master["url"], "/files/cv/Candidate_Master_CV.pdf")

    def test_document_resolution_rejects_traversal_and_non_pdf_files(self) -> None:
        pdf = self.repo / "cv" / "Candidate_Master_CV.pdf"
        pdf.write_bytes(PDF_BYTES)
        docx = self.repo / "cv" / "External_AI_CV.docx"
        docx.write_bytes(make_docx_bytes())
        (self.repo / "secret.pdf").write_bytes(PDF_BYTES)

        self.assertEqual(resolve_document_path(self.repo, "cv/Candidate_Master_CV.pdf"), pdf.resolve())
        self.assertEqual(resolve_document_path(self.repo, "cv/External_AI_CV.docx"), docx.resolve())
        self.assertIsNone(resolve_document_path(self.repo, "cv/../secret.pdf"))
        self.assertIsNone(resolve_document_path(self.repo, "../secret.pdf"))
        self.assertIsNone(resolve_document_path(self.repo, "cv/main_example.tex"))
        self.assertIsNone(resolve_document_path(self.repo, "secret.pdf"))

    def test_nested_document_resolution_keeps_security_boundaries(self):
        nested = self.repo / 'cv' / 'exampleco' / 'Candidate_CV.pdf'
        nested.parent.mkdir()
        nested.write_bytes(PDF_BYTES)
        self.assertEqual(resolve_document_path(self.repo, 'cv/exampleco/Candidate_CV.pdf'), nested.resolve())
        self.assertIsNone(resolve_document_path(self.repo, 'cv/exampleco/%2e%2e/Candidate_Master_CV.pdf'))
        self.assertIsNone(resolve_document_path(self.repo, 'cv/exampleco/file.tex'))
        secret = self.repo / 'secret.pdf'
        secret.write_bytes(PDF_BYTES)
        (nested.parent / 'outside.pdf').symlink_to(secret)
        self.assertIsNone(resolve_document_path(self.repo, 'cv/exampleco/outside.pdf'))
        self.assertIsNone(resolve_document_path(self.repo, 'cv/exampleco/..%5csecret.pdf'))

    def test_saves_valid_uploads_without_overwriting_existing_cv(self) -> None:
        pdf = save_uploaded_cv(self.repo, "External AI CV.pdf", PDF_BYTES)
        docx = save_uploaded_cv(self.repo, "External_AI_CV.docx", make_docx_bytes())

        self.assertEqual(pdf.read_bytes(), PDF_BYTES)
        self.assertTrue(docx.is_file())
        with self.assertRaisesRegex(FileExistsError, "already exists"):
            save_uploaded_cv(self.repo, "External AI CV.pdf", PDF_BYTES)
        with self.assertRaisesRegex(ValueError, "Only PDF and DOCX"):
            save_uploaded_cv(self.repo, "resume.txt", b"plain text")
        with self.assertRaisesRegex(ValueError, "not a valid DOCX"):
            save_uploaded_cv(self.repo, "broken.docx", b"not a zip")
        with self.assertRaisesRegex(ValueError, "must not contain a path"):
            save_uploaded_cv(self.repo, "../escape.pdf", PDF_BYTES)

    def test_server_allows_only_loopback_hosts(self) -> None:
        self.assertTrue(is_loopback_host("127.0.0.1"))
        self.assertTrue(is_loopback_host("::1"))
        self.assertTrue(is_loopback_host("localhost"))
        self.assertFalse(is_loopback_host("0.0.0.0"))
        self.assertFalse(is_loopback_host("192.0.2.10"))

    def test_discovers_pipeline_from_application_tracker(self) -> None:
        (self.repo / "job_search_tracker.csv").write_text(
            "date,company,sector,role,role_type,channel,status,contact_person,fit_rating,notes,cv_file,cover_letter_file,source\n"
            "2026-09-01,CloudExample,Cloud,Solutions & Integrations Engineer,Solutions,LinkedIn,interview,,high,"
            '"2026-09-20: Interview scheduled.",cv/test.tex,,https://il.linkedin.com/jobs/view/example-123\n'
            "2026-09-02,DeliveryExample,SaaS,Delivery Solution Engineer,Delivery,Referral,applied,,high,"
            '"Application submitted.",cv/test.tex,,https://www.delivery.example.com/careers/example/\n'
            "2026-09-03,Acme,SaaS,Implementation Manager,Delivery,Online,rejected,,medium,"
            '"2026-09-18: Rejected.",cv/test.tex,,https://example.com/jobs/123\n'
            "2026-09-04,Unsafe,SaaS,Unsafe Role,Delivery,Online,applied,,low,"
            '"Application submitted.",cv/test.tex,,javascript:alert(1)\n',
            encoding="utf-8",
        )

        applications = discover_application_links(self.repo)

        self.assertEqual(len(applications), 4)
        by_company = {application["company"]: application for application in applications}
        self.assertEqual(by_company["DeliveryExample"]["job_url"], "https://www.delivery.example.com/careers/example/")
        application = by_company["CloudExample"]
        self.assertEqual(application["company"], "CloudExample")
        self.assertEqual(application["job_url"], "https://il.linkedin.com/jobs/view/example-123")
        self.assertEqual(application["category"], "updates")
        self.assertEqual(application["status_label"], "Interview")
        self.assertEqual(application["last_update"], "2026-09-20")
        self.assertEqual(by_company["DeliveryExample"]["category"], "applied")
        self.assertEqual(by_company["Acme"]["category"], "rejected")
        self.assertEqual(by_company["Unsafe"]["job_url"], "")
        recruiter_query = parse_qs(urlsplit(application["recruiter_url"]).query)
        team_query = parse_qs(urlsplit(application["team_url"]).query)
        self.assertEqual(recruiter_query["keywords"], ["CloudExample recruiter"])
        self.assertEqual(team_query["keywords"], ["CloudExample Solutions & Integrations Engineer"])

    def test_malformed_or_missing_tracker_has_no_links(self) -> None:
        self.assertEqual(discover_application_links(self.repo), [])
        (self.repo / "job_search_tracker.csv").write_text("not a tracker", encoding="utf-8")
        self.assertEqual(discover_application_links(self.repo), [])

    def test_attaches_explicit_and_inferred_tailored_cv_downloads(self) -> None:
        explicit_name = "Candidate_CV_custom_role.pdf"
        inferred_name = "Candidate_CV_northstar_technical_account_manager.pdf"
        (self.repo / "cv" / explicit_name).write_bytes(PDF_BYTES)
        (self.repo / "cv" / inferred_name).write_bytes(PDF_BYTES)
        with (self.repo / "job_search_tracker.csv").open("w", encoding="utf-8", newline="") as target:
            writer = csv.DictWriter(
                target,
                fieldnames=(
                    "date", "company", "sector", "role", "role_type", "channel", "status",
                    "contact_person", "fit_rating", "notes", "cv_file", "cover_letter_file", "source",
                ),
            )
            writer.writeheader()
            writer.writerows(
                [
                    {
                        "date": "2026-10-01",
                        "company": "Explicit Co",
                        "role": "Different Public Title",
                        "status": "applied",
                        "fit_rating": "high",
                        "cv_file": "cv/Candidate_CV_custom_role.tex",
                        "source": "https://example.com/explicit",
                    },
                    {
                        "date": "2026-10-02",
                        "company": "Northstar",
                        "role": "Technical Account Manager",
                        "status": "applied",
                        "fit_rating": "high",
                        "source": "https://example.com/inferred",
                    },
                    {
                        "date": "2026-10-03",
                        "company": "No Match Ltd",
                        "role": "Unrelated Position",
                        "status": "applied",
                        "fit_rating": "medium",
                        "source": "https://example.com/unmatched",
                    },
                ]
            )

        applications = discover_application_links(self.repo)
        by_company = {application["company"]: application for application in applications}

        self.assertEqual(by_company["Explicit Co"]["cv_filename"], explicit_name)
        self.assertEqual(by_company["Explicit Co"]["cv_url"], f"/files/cv/{explicit_name}")
        self.assertEqual(by_company["Northstar"]["cv_filename"], inferred_name)
        self.assertEqual(by_company["Northstar"]["cv_url"], f"/files/cv/{inferred_name}")
        self.assertEqual(by_company["No Match Ltd"]["cv_url"], "")
        self.assertNotIn("cv_reference", by_company["Explicit Co"])

    def test_discovers_high_and_medium_suggestions_and_deduplicates_applications(self) -> None:
        (self.repo / "job_search_tracker.csv").write_text(
            "date,company,sector,role,role_type,channel,status,contact_person,fit_rating,notes,cv_file,cover_letter_file,source\n"
            "2026-09-01,Tracked Co,SaaS,Solutions Engineer,Solutions,Online,applied,,high,Submitted,,,https://example.com/tracked\n",
            encoding="utf-8",
        )
        scraper = self.repo / "job_scraper"
        scraper.mkdir()
        (scraper / "seen_jobs.json").write_text(
            json.dumps(
                {
                    "seen": {
                        "suggested-high": {
                            "title": "Pre-Sale Expert",
                            "company": "TransitExample",
                            "url": "https://www.linkedin.com/jobs/view/123",
                            "first_seen": "2026-09-29",
                            "fit": "high",
                            "status": "new",
                        },
                        "suggested-medium": {
                            "title": "Technical Account Manager",
                            "company": "Northstar",
                            "url": "https://example.com/northstar",
                            "first_seen": "2026-09-28",
                            "fit": "medium",
                            "status": "ranked",
                            "rank_verdict": "Good technical onboarding alignment",
                        },
                        "duplicate": {
                            "title": "Solutions Engineer Israel",
                            "company": "Tracked Co",
                            "url": "https://example.com/tracked",
                            "first_seen": "2026-09-29",
                            "fit": "high",
                            "status": "evaluated",
                        },
                        "low": {
                            "title": "Unrelated Role",
                            "company": "Low Co",
                            "url": "https://example.com/low",
                            "first_seen": "2026-09-29",
                            "fit": "low",
                            "status": "new",
                        },
                    }
                }
            ),
            encoding="utf-8",
        )

        applications = discover_application_links(self.repo)

        self.assertEqual(len(applications), 3)
        suggestions = [item for item in applications if item["category"] == "suggested"]
        self.assertEqual({item["company"] for item in suggestions}, {"TransitExample", "Northstar"})
        self.assertEqual(suggestions[0]["date_label"], "Added")
        northstar = next(item for item in suggestions if item["company"] == "Northstar")
        self.assertEqual(northstar["notes"], "Good technical onboarding alignment")

    def test_marks_suggested_job_applied_once(self) -> None:
        scraper = self.repo / "job_scraper"
        scraper.mkdir()
        job_url = "https://www.linkedin.com/jobs/view/4336023200"
        (scraper / "seen_jobs.json").write_text(
            json.dumps(
                {
                    "seen": {
                        job_url: {
                            "title": "Solution Expert",
                            "company": "DealHub.ai",
                            "url": job_url,
                            "first_seen": "2026-09-29",
                            "fit": "medium",
                            "status": "new",
                        }
                    }
                }
            ),
            encoding="utf-8",
        )

        first = mark_suggested_job_applied(self.repo, job_url, today="2026-09-29")
        second = mark_suggested_job_applied(self.repo, job_url, today="2026-09-29")

        self.assertEqual(first["status"], "applied")
        self.assertEqual(second["status"], "already_applied")
        with (self.repo / "job_search_tracker.csv").open(encoding="utf-8", newline="") as source:
            rows = list(csv.DictReader(source))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["company"], "DealHub.ai")
        self.assertEqual(rows[0]["role"], "Solution Expert")
        self.assertEqual(rows[0]["status"], "applied")
        self.assertEqual(rows[0]["source"], job_url)


class CVWebViewHTTPTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo = Path(self.temp_dir.name)
        (self.repo / "cv").mkdir()
        (self.repo / "cover_letters").mkdir()
        (self.repo / "cv" / "Candidate_Master_CV.pdf").write_bytes(PDF_BYTES)
        self.server = create_server(self.repo, port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temp_dir.cleanup()

    def test_home_page_and_api(self) -> None:
        with urlopen(f"{self.base_url}/", timeout=2) as response:
            html = response.read().decode("utf-8")
            self.assertIn("Application documents", html)
            self.assertIn("Application pipeline", html)
            self.assertIn("Suggested", html)
            self.assertIn("New updates", html)
            self.assertIn("Mark applied", html)
            self.assertIn("Search jobs, companies, statuses, CVs", html)
            self.assertIn("PDF and DOCX files stored in this private workspace", html)
            self.assertIn("Upload CV", html)
            self.assertIn("CV ↓", html)
            self.assertNotIn("<iframe", html)
            self.assertIn("default-src 'self'", response.headers["Content-Security-Policy"])

        with urlopen(f"{self.base_url}/api/documents", timeout=2) as response:
            payload = json.load(response)
            self.assertEqual(payload["documents"][0]["id"], "cv/Candidate_Master_CV.pdf")
            self.assertEqual(payload["applications"], [])

    def test_upload_endpoint_saves_cv_and_rejects_duplicate(self) -> None:
        filename = "External AI Résumé.pdf"
        request = Request(
            f"{self.base_url}/api/cv/upload",
            data=PDF_BYTES,
            headers={
                "Content-Type": "application/pdf",
                "X-Filename": quote(filename),
            },
            method="POST",
        )

        with urlopen(request, timeout=2) as response:
            result = json.load(response)
            self.assertEqual(response.status, 201)
        self.assertEqual(result, {"status": "uploaded", "filename": filename, "folder": "cv"})
        self.assertEqual((self.repo / "cv" / filename).read_bytes(), PDF_BYTES)

        with urlopen(f"{self.base_url}/api/documents", timeout=2) as response:
            payload = json.load(response)
        uploaded = next(document for document in payload["documents"] if document["filename"] == filename)
        self.assertEqual(uploaded["format"], "PDF")
        with urlopen(f"{self.base_url}{uploaded['url']}", timeout=2) as response:
            self.assertEqual(response.read(), PDF_BYTES)
            self.assertIn("filename*=UTF-8''", response.headers["Content-Disposition"])

        with self.assertRaises(HTTPError) as raised:
            urlopen(request, timeout=2)
        self.assertEqual(raised.exception.code, 409)
        self.assertIn("already exists", json.loads(raised.exception.read())["error"])
        raised.exception.close()

    def test_mark_applied_endpoint_updates_pipeline(self) -> None:
        scraper = self.repo / "job_scraper"
        scraper.mkdir()
        job_url = "https://example.com/jobs/implementation"
        (scraper / "seen_jobs.json").write_text(
            json.dumps(
                {
                    "seen": {
                        job_url: {
                            "title": "Implementation Manager",
                            "company": "Example Co",
                            "url": job_url,
                            "first_seen": "2026-09-29",
                            "fit": "high",
                            "status": "new",
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        request = Request(
            f"{self.base_url}/api/applications/mark-applied",
            data=json.dumps({"job_url": job_url}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with urlopen(request, timeout=2) as response:
            result = json.load(response)
        with urlopen(f"{self.base_url}/api/documents", timeout=2) as response:
            payload = json.load(response)

        self.assertEqual(result["status"], "applied")
        self.assertEqual(len(payload["applications"]), 1)
        self.assertEqual(payload["applications"][0]["category"], "applied")

    def test_mark_applied_endpoint_returns_json_for_unexpected_server_error(self) -> None:
        request = Request(
            f"{self.base_url}/api/applications/mark-applied",
            data=json.dumps({"job_url": "https://example.com/jobs/implementation"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with patch("tools.cv_webview.mark_suggested_job_applied", side_effect=RuntimeError("test failure")):
            with self.assertRaises(HTTPError) as raised:
                urlopen(request, timeout=2)

        self.assertEqual(raised.exception.code, 500)
        self.assertEqual(
            json.loads(raised.exception.read()),
            {"error": "Could not update application tracker"},
        )
        raised.exception.close()

    def test_pdf_inline_download_and_byte_range(self) -> None:
        with urlopen(f"{self.base_url}/files/cv/Candidate_Master_CV.pdf", timeout=2) as response:
            self.assertEqual(response.read(), PDF_BYTES)
            self.assertEqual(response.headers.get_content_type(), "application/pdf")
            self.assertTrue(response.headers["Content-Disposition"].startswith("inline"))

        with urlopen(f"{self.base_url}/files/cv/Candidate_Master_CV.pdf?download=1", timeout=2) as response:
            self.assertTrue(response.headers["Content-Disposition"].startswith("attachment"))

        nested = self.repo / 'cv' / 'exampleco' / 'Candidate_CV.pdf'
        nested.parent.mkdir()
        nested.write_bytes(PDF_BYTES)
        for suffix, disposition in (('', 'inline'), ('?download=1', 'attachment')):
            with urlopen(f'{self.base_url}/files/cv/exampleco/Candidate_CV.pdf{suffix}', timeout=2) as response:
                self.assertEqual(response.read(), PDF_BYTES)
                self.assertTrue(response.headers['Content-Disposition'].startswith(disposition))

        request = Request(f"{self.base_url}/files/cv/Candidate_Master_CV.pdf", headers={"Range": "bytes=0-3"})
        with urlopen(request, timeout=2) as response:
            self.assertEqual(response.status, 206)
            self.assertEqual(response.read(), b"%PDF")
            self.assertEqual(response.headers["Content-Range"], f"bytes 0-3/{len(PDF_BYTES)}")

    def test_file_endpoint_rejects_path_traversal(self) -> None:
        with self.assertRaises(HTTPError) as context:
            urlopen(f"{self.base_url}/files/cv/%2e%2e%2fsecret.pdf", timeout=2)
        self.assertEqual(context.exception.code, 404)
        context.exception.close()


if __name__ == "__main__":
    unittest.main()
