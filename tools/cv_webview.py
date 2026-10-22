#!/usr/bin/env python3
"""Private browser viewer for generated CVs and cover letters.

The server intentionally binds to loopback only. Use an SSH local-forward from
the client machine instead of exposing career documents on the network.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import hmac
import io
import ipaddress
import json
import mimetypes
import os
import re
import sys
import tempfile
import threading
import time
import zipfile
from datetime import datetime
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from urllib.parse import parse_qs, quote, unquote, urlencode, urlsplit

try:
    from tools.portal_codex import CodexRPCError, CodexUnavailable
    from tools.portal_store import ConflictError, NotFoundError, PortalStore, ValidationError
    from tools.portal_ui import PORTAL_HTML
    from tools.portal_worker import PortalWorker
except ModuleNotFoundError:  # Direct `python3 tools/cv_webview.py` execution.
    from portal_codex import CodexRPCError, CodexUnavailable
    from portal_store import ConflictError, NotFoundError, PortalStore, ValidationError
    from portal_ui import PORTAL_HTML
    from portal_worker import PortalWorker


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
DOCUMENT_DIRECTORIES = {"cv": "CV", "cover_letters": "Cover Letter"}
DOCUMENT_EXTENSIONS = {".pdf", ".docx"}
MAX_UPLOAD_BYTES = 20 * 1024 * 1024
RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")
DATE_RE = re.compile(r"\b20\d{2}-\d{2}-\d{2}\b")
APPLIED_STATUSES = {"applied", "submitted", "pending"}
REJECTED_STATUSES = {"rejected"}
TRACKER_FIELDS = (
    "date",
    "company",
    "sector",
    "role",
    "role_type",
    "channel",
    "status",
    "contact_person",
    "fit_rating",
    "notes",
    "cv_file",
    "cover_letter_file",
    "source",
)
TRACKER_WRITE_LOCK = threading.Lock()
UPLOAD_WRITE_LOCK = threading.Lock()


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Application Documents</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #071018;
      --panel: #0c1823;
      --panel-2: #101f2d;
      --panel-3: #142738;
      --line: #203548;
      --line-strong: #33516a;
      --text: #e8f1f8;
      --muted: #91a8ba;
      --blue: #65aefa;
      --blue-2: #2e7ecf;
      --green: #65d6ad;
      --amber: #f2cb84;
      --red: #ff9ca3;
      --shadow: 0 14px 42px rgba(0, 0, 0, .28);
    }
    * { box-sizing: border-box; }
    html, body { height: 100%; margin: 0; }
    body {
      background: radial-gradient(circle at 20% 0%, #122b3e 0, var(--bg) 38%);
      color: var(--text);
      font: 14px/1.45 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      overflow: hidden;
    }
    button, input, a { font: inherit; }
    button, a { -webkit-tap-highlight-color: transparent; }
    button { cursor: pointer; }
    .app { display: flex; flex-direction: column; height: 100dvh; min-height: 0; }
    .topbar {
      align-items: center; background: rgba(8, 20, 30, .94); border-bottom: 1px solid var(--line);
      backdrop-filter: blur(18px); display: grid; gap: 18px; grid-template-columns: minmax(230px, .55fr) minmax(320px, 1.45fr) auto;
      padding: 14px 20px; z-index: 3;
    }
    .brand { min-width: 0; }
    .eyebrow { color: var(--green); font-size: 11px; font-weight: 800; letter-spacing: .16em; text-transform: uppercase; }
    h1 { font-size: 20px; line-height: 1.15; margin: 4px 0 0; }
    .search-wrap { min-width: 0; position: relative; }
    .search-icon { color: var(--muted); font-size: 17px; left: 14px; pointer-events: none; position: absolute; top: 50%; transform: translateY(-50%); }
    .search {
      width: 100%; border: 1px solid var(--line); border-radius: 12px; outline: none;
      background: var(--panel); color: var(--text); padding: 11px 42px 11px 41px;
    }
    .search:focus { border-color: var(--blue); box-shadow: 0 0 0 3px rgba(101, 174, 250, .12); }
    .search-shortcut {
      border: 1px solid var(--line); border-radius: 6px; color: var(--muted); font-size: 10px; padding: 2px 6px;
      position: absolute; right: 12px; top: 50%; transform: translateY(-50%);
    }
    .topbar-actions { align-items: center; display: flex; gap: 8px; }
    .filters { display: flex; flex-wrap: wrap; gap: 7px; }
    .filter, .icon-button {
      border: 1px solid var(--line); border-radius: 999px; background: transparent; color: var(--muted);
      padding: 7px 11px;
    }
    .filter:hover, .icon-button:hover { color: var(--text); border-color: #39536a; }
    .filter.active { color: #071018; background: var(--blue); border-color: var(--blue); font-weight: 750; }
    .icon-button.square { border-radius: 10px; min-height: 38px; padding: 8px 12px; }
    .icon-button.upload { border-color: rgba(101, 214, 173, .48); color: var(--green); }
    .icon-button.upload:hover { background: rgba(101, 214, 173, .10); }
    .icon-button:disabled { cursor: wait; opacity: .6; }
    .status-bar {
      align-items: center; background: rgba(12, 24, 35, .82); border-bottom: 1px solid var(--line); color: var(--muted);
      display: flex; font-size: 12px; gap: 10px; justify-content: space-between; min-height: 34px; padding: 6px 20px;
    }
    .status-bar strong { color: var(--text); }
    .document-panel { border-bottom: 1px solid var(--line); flex: 0 0 auto; padding: 12px 20px 14px; }
    .document-panel[hidden] { display: none; }
    .section-heading { align-items: center; display: flex; gap: 14px; justify-content: space-between; margin-bottom: 9px; }
    .section-heading-copy { align-items: baseline; display: flex; gap: 9px; min-width: 0; }
    .section-heading h2, .pipeline-heading h2 { font-size: 14px; margin: 0; }
    .section-heading span, .pipeline-heading span { color: var(--muted); font-size: 11px; }
    .document-list {
      display: grid; gap: 9px; grid-auto-columns: minmax(255px, 300px); grid-auto-flow: column;
      overflow-x: auto; overscroll-behavior-inline: contain; padding: 1px 0 5px; scrollbar-width: thin;
    }
    .document-card {
      background: linear-gradient(145deg, rgba(20, 42, 61, .86), rgba(12, 24, 35, .96)); border: 1px solid var(--line);
      border-radius: 12px; color: inherit; display: flex; flex-direction: column; min-height: 96px; min-width: 0; padding: 10px 11px;
    }
    .document-card:hover { border-color: var(--line-strong); transform: translateY(-1px); }
    .document-title {
      display: -webkit-box; font-size: 12px; font-weight: 760; line-height: 1.35; min-height: 32px; overflow: hidden;
      -webkit-box-orient: vertical; -webkit-line-clamp: 2;
    }
    .document-meta { color: var(--muted); display: flex; flex-wrap: wrap; gap: 5px 8px; margin-top: 5px; font-size: 10px; }
    .pill { border: 1px solid var(--line); border-radius: 999px; padding: 1px 7px; }
    .document-actions { display: flex; gap: 6px; margin-top: auto; padding-top: 8px; }
    .document-link {
      border: 1px solid var(--line); border-radius: 7px; color: var(--text); font-size: 10px; padding: 4px 8px; text-decoration: none;
    }
    .document-link:hover { background: var(--panel-3); border-color: var(--line-strong); }
    .document-link.primary { background: rgba(46, 126, 207, .20); border-color: var(--blue-2); color: var(--blue); }
    .document-empty { color: var(--muted); grid-column: 1 / -1; padding: 18px 4px; }
    .pipeline-panel {
      background: rgba(8, 20, 30, .56); display: flex; flex: 1; flex-direction: column; min-height: 0; padding: 14px 20px 18px;
    }
    .pipeline-panel[hidden] { display: none; }
    .pipeline-heading { align-items: baseline; display: flex; gap: 9px; justify-content: space-between; margin-bottom: 11px; }
    .pipeline-heading-copy { align-items: baseline; display: flex; gap: 9px; }
    .pipeline-board {
      display: grid; flex: 1; gap: 12px; grid-template-columns: repeat(4, minmax(320px, 1fr)); min-height: 0;
      overflow-x: auto; overscroll-behavior-inline: contain; padding-bottom: 3px; scrollbar-width: thin;
    }
    .pipeline-column {
      background: rgba(12, 24, 35, .88); border: 1px solid var(--line); border-radius: 13px; display: flex;
      flex-direction: column; min-height: 0; min-width: 320px; overflow: hidden;
    }
    .pipeline-column[data-category="suggested"] { border-top: 3px solid var(--green); }
    .pipeline-column[data-category="updates"] { border-top: 3px solid var(--amber); }
    .pipeline-column[data-category="applied"] { border-top: 3px solid var(--blue); }
    .pipeline-column[data-category="rejected"] { border-top: 3px solid var(--red); }
    .pipeline-column-header {
      align-items: center; background: rgba(16, 31, 45, .94); border-bottom: 1px solid var(--line); display: flex;
      flex: 0 0 auto; gap: 8px; justify-content: space-between; padding: 11px 12px;
    }
    .pipeline-column-title { font-size: 12px; font-weight: 820; letter-spacing: .07em; text-transform: uppercase; }
    .pipeline-column-subtitle { color: var(--muted); font-size: 10px; margin-top: 2px; }
    .pipeline-count {
      align-items: center; background: var(--panel-2); border: 1px solid var(--line); border-radius: 999px; display: inline-flex;
      font-size: 11px; font-weight: 800; height: 24px; justify-content: center; min-width: 24px; padding: 0 7px;
    }
    .pipeline-list { align-content: start; display: grid; flex: 1; gap: 9px; min-height: 0; overflow-y: auto; padding: 10px; scrollbar-width: thin; }
    .application-card {
      background: var(--panel); border: 1px solid var(--line); border-radius: 11px; box-shadow: 0 7px 18px rgba(0, 0, 0, .12);
      min-width: 0; padding: 11px 12px;
    }
    .application-card[data-category="suggested"] { background: linear-gradient(135deg, rgba(101, 214, 173, .10), var(--panel)); }
    .application-card[data-category="updates"] { background: linear-gradient(135deg, rgba(233, 184, 95, .10), var(--panel)); }
    .application-card[data-category="rejected"] { background: linear-gradient(135deg, rgba(239, 123, 131, .09), var(--panel)); }
    .application-topline { align-items: center; display: flex; gap: 8px; justify-content: space-between; }
    .application-company {
      color: var(--green); font-size: 10px; font-weight: 820; letter-spacing: .055em; overflow: hidden;
      text-overflow: ellipsis; text-transform: uppercase; white-space: nowrap;
    }
    .status-badge { border: 1px solid var(--line); border-radius: 999px; flex: 0 0 auto; font-size: 10px; font-weight: 780; padding: 2px 7px; }
    .status-badge[data-category="suggested"] { border-color: rgba(101, 214, 173, .55); color: var(--green); }
    .status-badge[data-category="updates"] { border-color: rgba(233, 184, 95, .55); color: var(--amber); }
    .status-badge[data-category="applied"] { border-color: rgba(101, 174, 250, .55); color: var(--blue); }
    .status-badge[data-category="rejected"] { border-color: rgba(239, 123, 131, .55); color: var(--red); }
    .application-title { font-size: 13px; font-weight: 760; line-height: 1.35; margin: 5px 0 6px; }
    .application-meta { color: var(--muted); display: flex; flex-wrap: wrap; font-size: 10px; gap: 4px 9px; margin-bottom: 6px; }
    .application-notes {
      color: #bfd0dc; display: -webkit-box; font-size: 11px; line-height: 1.4; margin: 0 0 9px; overflow: hidden;
      -webkit-box-orient: vertical; -webkit-line-clamp: 3;
    }
    .application-actions { display: flex; flex-wrap: wrap; gap: 6px; }
    .application-link {
      border: 1px solid var(--line); border-radius: 7px; color: var(--text); font-size: 10px; padding: 5px 7px;
      text-decoration: none;
    }
    .application-link:hover { background: var(--panel-2); border-color: #39536a; }
    .application-link.primary { border-color: var(--blue-2); color: var(--blue); }
    .application-link.cv { background: rgba(101, 214, 173, .10); border-color: rgba(101, 214, 173, .48); color: var(--green); }
    .application-button {
      background: transparent; border: 1px solid var(--line); border-radius: 7px; color: var(--text); cursor: pointer;
      font-size: 10px; padding: 5px 8px;
    }
    .application-button:hover { background: var(--panel-2); border-color: #39536a; }
    .application-button.primary { background: rgba(101, 214, 173, .12); border-color: rgba(101, 214, 173, .55); color: var(--green); }
    .application-button:disabled { cursor: wait; opacity: .65; }
    .pipeline-empty { color: var(--muted); font-size: 11px; padding: 26px 10px; text-align: center; }
    @media (max-width: 1400px) {
      .pipeline-board { grid-auto-columns: minmax(340px, 43vw); grid-auto-flow: column; grid-template-columns: none; }
    }
    @media (max-width: 820px) {
      body { overflow: auto; }
      .app { height: auto; min-height: 100dvh; }
      .topbar { gap: 10px; grid-template-columns: 1fr auto; padding: 12px; }
      .search-wrap { grid-column: 1 / -1; grid-row: 2; }
      .topbar-actions .label { display: none; }
      .status-bar { align-items: flex-start; flex-direction: column; padding: 6px 12px; }
      .document-panel { padding: 11px 12px 13px; }
      .section-heading { align-items: flex-start; flex-direction: column; gap: 7px; }
      .document-list { grid-auto-columns: minmax(235px, 78vw); }
      .pipeline-panel { min-height: 660px; padding: 12px; }
      .pipeline-heading { align-items: flex-start; flex-direction: column; }
      .pipeline-board { grid-auto-columns: minmax(285px, 86vw); }
      .pipeline-column { min-height: 590px; min-width: 285px; }
    }
  </style>
</head>
<body>
  <main class="app">
    <header class="topbar">
      <div class="brand">
        <div class="eyebrow">Private local viewer</div>
        <h1>Application documents</h1>
      </div>
      <div class="search-wrap">
        <span class="search-icon" aria-hidden="true">⌕</span>
        <input id="search" class="search" type="search" placeholder="Search jobs, companies, statuses, CVs…" autocomplete="off" aria-label="Search jobs and documents">
        <span class="search-shortcut" aria-hidden="true">/</span>
      </div>
      <div class="topbar-actions">
        <input id="upload-input" type="file" accept=".pdf,.docx,application/pdf,application/vnd.openxmlformats-officedocument.wordprocessingml.document" multiple hidden>
        <button id="upload" class="icon-button square upload" type="button" title="Upload PDF or DOCX CVs"><span class="label">Upload CV</span> ↑</button>
        <button id="toggle-documents" class="icon-button square" type="button" aria-expanded="true"><span class="label">Hide documents</span> ▴</button>
        <button id="refresh" class="icon-button square" type="button" title="Refresh" aria-label="Refresh">↻</button>
      </div>
    </header>

    <div class="status-bar">
      <span id="status">Loading…</span>
      <span id="search-summary">Search covers jobs and documents · Refreshes automatically</span>
    </div>

    <section id="document-panel" class="document-panel" aria-labelledby="documents-heading">
      <div class="section-heading">
        <div class="section-heading-copy">
          <h2 id="documents-heading">Document library</h2>
          <span>PDF and DOCX files stored in this private workspace</span>
        </div>
        <div class="filters" aria-label="Document type">
          <button class="filter active" data-filter="all">All</button>
          <button class="filter" data-filter="cv">CVs</button>
          <button class="filter" data-filter="cover_letter">Letters</button>
        </div>
      </div>
      <nav id="documents" class="document-list" aria-label="Documents"></nav>
    </section>

    <section id="pipeline-panel" class="pipeline-panel" aria-labelledby="pipeline-heading" hidden>
      <div class="pipeline-heading">
        <div class="pipeline-heading-copy">
          <h2 id="pipeline-heading">Application pipeline</h2>
          <span>Suggested roles and your private application tracker</span>
        </div>
        <span id="pipeline-total"></span>
      </div>
      <div id="pipeline-board" class="pipeline-board"></div>
    </section>
  </main>

  <script>
    const state = { documents: [], applications: [], filter: 'all', query: '', documentsExpanded: true };
    const list = document.getElementById('documents');
    const documentPanel = document.getElementById('document-panel');
    const pipelinePanel = document.getElementById('pipeline-panel');
    const pipelineBoard = document.getElementById('pipeline-board');
    const pipelineTotal = document.getElementById('pipeline-total');
    const status = document.getElementById('status');
    const search = document.getElementById('search');
    const searchSummary = document.getElementById('search-summary');
    const toggleDocuments = document.getElementById('toggle-documents');
    const uploadButton = document.getElementById('upload');
    const uploadInput = document.getElementById('upload-input');

    function formatSize(bytes) {
      if (bytes < 1024) return `${bytes} B`;
      if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
      return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
    }
    function formatDate(value) {
      return new Intl.DateTimeFormat(undefined, { dateStyle: 'medium' }).format(new Date(value));
    }
    function formatTrackerDate(value) {
      return new Intl.DateTimeFormat(undefined, { dateStyle: 'medium' }).format(new Date(`${value}T00:00:00`));
    }
    function matchesQuery(value) {
      const haystack = String(value || '').toLowerCase();
      const terms = state.query.trim().toLowerCase().split(/\s+/).filter(Boolean);
      return terms.every(term => haystack.includes(term));
    }
    function visibleDocuments() {
      return state.documents.filter(doc =>
        (state.filter === 'all' || doc.kind === state.filter) &&
        matchesQuery(`${doc.title} ${doc.filename} ${doc.kind_label}`)
      );
    }
    function visibleApplications() {
      return state.applications.filter(application =>
        matchesQuery(`${application.company} ${application.title} ${application.status_label} ${application.category} ${application.notes} ${application.channel} ${application.fit} ${application.last_update} ${application.cv_filename}`)
      );
    }
    function makeDocumentLink(label, href, primary = false, download = '') {
      const link = document.createElement('a');
      link.className = `document-link${primary ? ' primary' : ''}`;
      link.href = href;
      link.target = download ? '_self' : '_blank';
      link.rel = 'noopener noreferrer';
      link.textContent = label;
      if (download) link.download = download;
      return link;
    }
    function renderList() {
      const previousScroll = list.scrollLeft;
      list.replaceChildren();
      const documents = visibleDocuments();
      if (!documents.length) {
        const blank = document.createElement('div');
        blank.className = 'document-empty';
        blank.textContent = state.query ? 'No documents match this search.' : 'No CV documents found.';
        list.appendChild(blank);
      }
      for (const doc of documents) {
        const card = document.createElement('article');
        card.className = 'document-card';

        const documentTitle = document.createElement('div');
        documentTitle.className = 'document-title';
        documentTitle.textContent = doc.title;
        documentTitle.title = doc.title;

        const documentMeta = document.createElement('div');
        documentMeta.className = 'document-meta';
        const pill = document.createElement('span');
        pill.className = 'pill';
        pill.textContent = `${doc.kind_label} · ${doc.format}`;
        const size = document.createElement('span');
        size.textContent = formatSize(doc.size);
        const modified = document.createElement('span');
        modified.textContent = formatDate(doc.modified);
        documentMeta.append(pill, size, modified);

        const actions = document.createElement('div');
        actions.className = 'document-actions';
        actions.append(
          makeDocumentLink('Open ↗', doc.url, true),
          makeDocumentLink('Download ↓', `${doc.url}?download=1`, false, doc.filename),
        );
        card.append(documentTitle, documentMeta, actions);
        list.appendChild(card);
      }
      list.scrollLeft = previousScroll;
      renderSummary();
    }
    function renderSummary() {
      const documentCount = `${state.documents.length} document${state.documents.length === 1 ? '' : 's'}`;
      const trackedCount = state.applications.filter(item => item.category !== 'suggested').length;
      const suggestedCount = state.applications.filter(item => item.category === 'suggested').length;
      status.textContent = `${documentCount} · ${trackedCount} application${trackedCount === 1 ? '' : 's'} · ${suggestedCount} suggestion${suggestedCount === 1 ? '' : 's'}`;
      const q = state.query.trim();
      if (q) {
        const matchingDocuments = visibleDocuments().length;
        const matchingApplications = visibleApplications().length;
        searchSummary.textContent = `${matchingApplications} job${matchingApplications === 1 ? '' : 's'} · ${matchingDocuments} document${matchingDocuments === 1 ? '' : 's'} matching “${q}”`;
      } else {
        searchSummary.textContent = 'Search covers jobs and documents · Press / to focus';
      }
    }
    function makeApplicationLink(label, href, primary = false) {
      const link = document.createElement('a');
      link.className = `application-link${primary ? ' primary' : ''}`;
      link.href = href;
      link.target = '_blank';
      link.rel = 'noopener noreferrer';
      link.textContent = label;
      return link;
    }
    function makeCVDownload(application) {
      const link = makeApplicationLink('CV ↓', `${application.cv_url}?download=1`);
      link.classList.add('cv');
      link.target = '_self';
      link.download = application.cv_filename;
      link.title = `Download ${application.cv_filename}`;
      return link;
    }
    async function markApplied(application, button) {
      const original = button.textContent;
      button.disabled = true;
      button.textContent = 'Saving…';
      try {
        const response = await fetch('/api/applications/mark-applied', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ job_url: application.job_url }),
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
        button.textContent = 'Applied ✓';
        status.textContent = `${application.company} moved to Applied`;
        await loadDocuments();
      } catch (error) {
        button.disabled = false;
        button.textContent = original;
        const message = error instanceof Error ? error.message : 'Unknown error';
        status.textContent = `Could not update application: ${message}`;
        console.error(error);
      }
    }
    const pipelineGroups = [
      { category: 'suggested', title: 'Suggested', subtitle: 'High and medium-fit roles not yet applied to' },
      { category: 'updates', title: 'New updates', subtitle: 'Interviews, assessments, offers, and other movement' },
      { category: 'applied', title: 'Applied', subtitle: 'Submitted and waiting for a response' },
      { category: 'rejected', title: 'Rejected', subtitle: 'Closed after an explicit rejection' },
    ];
    function makeApplicationCard(application) {
      const card = document.createElement('article');
      card.className = 'application-card';
      card.dataset.category = application.category;

      const topline = document.createElement('div');
      topline.className = 'application-topline';

      const company = document.createElement('div');
      company.className = 'application-company';
      company.textContent = application.company;
      company.title = application.company;

      const badge = document.createElement('span');
      badge.className = 'status-badge';
      badge.dataset.category = application.category;
      badge.textContent = application.status_label;
      topline.append(company, badge);

      const role = document.createElement('div');
      role.className = 'application-title';
      role.textContent = application.title;
      role.title = application.title;

      const details = [];
      if (application.last_update) details.push(`${application.date_label || 'Updated'} ${formatTrackerDate(application.last_update)}`);
      if (application.channel) details.push(application.channel);
      if (application.fit) details.push(`${application.fit} fit`);
      const metaLine = document.createElement('div');
      metaLine.className = 'application-meta';
      for (const detail of details) {
        const item = document.createElement('span');
        item.textContent = detail;
        metaLine.appendChild(item);
      }

      card.append(topline, role);
      if (details.length) card.appendChild(metaLine);
      if (application.notes) {
        const notes = document.createElement('p');
        notes.className = 'application-notes';
        notes.textContent = application.notes;
        notes.title = application.notes;
        card.appendChild(notes);
      }

      const actions = document.createElement('div');
      actions.className = 'application-actions';
      if (application.category === 'suggested' && application.job_url) {
        const markButton = document.createElement('button');
        markButton.type = 'button';
        markButton.className = 'application-button primary';
        markButton.textContent = '✓ Mark applied';
        markButton.addEventListener('click', () => markApplied(application, markButton));
        actions.appendChild(markButton);
      }
      if (application.cv_url) actions.appendChild(makeCVDownload(application));
      if (application.job_url) actions.appendChild(makeApplicationLink('Job ↗', application.job_url, true));
      actions.append(
        makeApplicationLink('Recruiters ↗', application.recruiter_url),
        makeApplicationLink('Team ↗', application.team_url),
      );
      card.appendChild(actions);
      return card;
    }
    function renderApplications() {
      const boardScroll = pipelineBoard.scrollLeft;
      const columnScroll = new Map(
        Array.from(pipelineBoard.querySelectorAll('.pipeline-column')).map(column => [
          column.dataset.category,
          column.querySelector('.pipeline-list')?.scrollTop || 0,
        ])
      );
      pipelineBoard.replaceChildren();
      pipelinePanel.hidden = state.applications.length === 0;
      const visible = visibleApplications();
      const trackedCount = state.applications.filter(item => item.category !== 'suggested').length;
      const suggestedCount = state.applications.filter(item => item.category === 'suggested').length;
      pipelineTotal.textContent = state.query.trim()
        ? `${visible.length} of ${state.applications.length} roles`
        : `${trackedCount} applications · ${suggestedCount} suggestions`;
      for (const group of pipelineGroups) {
        const applications = visible.filter(item => item.category === group.category);
        const column = document.createElement('section');
        column.className = 'pipeline-column';
        column.dataset.category = group.category;

        const header = document.createElement('header');
        header.className = 'pipeline-column-header';
        const copy = document.createElement('div');
        const heading = document.createElement('div');
        heading.className = 'pipeline-column-title';
        heading.textContent = group.title;
        const subtitle = document.createElement('div');
        subtitle.className = 'pipeline-column-subtitle';
        subtitle.textContent = group.subtitle;
        copy.append(heading, subtitle);
        const count = document.createElement('span');
        count.className = 'pipeline-count';
        count.textContent = applications.length;
        header.append(copy, count);

        const groupList = document.createElement('div');
        groupList.className = 'pipeline-list';
        if (applications.length) {
          for (const application of applications) groupList.appendChild(makeApplicationCard(application));
        } else {
          const blank = document.createElement('div');
          blank.className = 'pipeline-empty';
          blank.textContent = state.query.trim()
            ? 'No matches in this column.'
            : group.category === 'updates' ? 'No new updates recorded yet.' : 'Nothing in this category.';
          groupList.appendChild(blank);
        }
        column.append(header, groupList);
        pipelineBoard.appendChild(column);
        groupList.scrollTop = columnScroll.get(group.category) || 0;
      }
      pipelineBoard.scrollLeft = boardScroll;
      renderSummary();
    }
    async function loadDocuments(manual = false) {
      if (manual) status.textContent = 'Refreshing…';
      try {
        const response = await fetch('/api/documents', { cache: 'no-store' });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const payload = await response.json();
        state.documents = payload.documents;
        state.applications = payload.applications || [];
        renderApplications();
        renderList();
      } catch (error) {
        status.textContent = 'Viewer unavailable';
        console.error(error);
      }
    }
    async function uploadCVs(files) {
      if (!files.length) return;
      uploadButton.disabled = true;
      uploadButton.querySelector('.label').textContent = 'Uploading';
      const uploaded = [];
      const failed = [];
      for (const file of files) {
        try {
          if (file.size > 20 * 1024 * 1024) throw new Error('larger than 20 MB');
          const response = await fetch('/api/cv/upload', {
            method: 'POST',
            headers: {
              'Content-Type': file.type || 'application/octet-stream',
              'X-Filename': encodeURIComponent(file.name),
            },
            body: file,
          });
          const payload = await response.json().catch(() => ({}));
          if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
          uploaded.push(payload.filename || file.name);
        } catch (error) {
          const message = error instanceof Error ? error.message : 'Unknown error';
          failed.push(`${file.name}: ${message}`);
        }
      }
      await loadDocuments();
      uploadButton.disabled = false;
      uploadButton.querySelector('.label').textContent = 'Upload CV';
      uploadInput.value = '';
      if (failed.length) {
        status.textContent = `${uploaded.length} uploaded · ${failed.join(' · ')}`;
      } else {
        status.textContent = `${uploaded.length} CV${uploaded.length === 1 ? '' : 's'} uploaded to cv/`;
      }
    }
    search.addEventListener('input', event => {
      state.query = event.target.value;
      renderList();
      renderApplications();
    });
    document.querySelectorAll('.filter').forEach(button => button.addEventListener('click', () => {
      state.filter = button.dataset.filter;
      document.querySelectorAll('.filter').forEach(item => item.classList.toggle('active', item === button));
      renderList();
    }));
    function updateDocumentPanel() {
      documentPanel.hidden = !state.documentsExpanded;
      toggleDocuments.setAttribute('aria-expanded', String(state.documentsExpanded));
      toggleDocuments.querySelector('.label').textContent = state.documentsExpanded ? 'Hide documents' : 'Show documents';
      toggleDocuments.lastChild.textContent = state.documentsExpanded ? ' ▴' : ' ▾';
    }
    toggleDocuments.addEventListener('click', () => {
      state.documentsExpanded = !state.documentsExpanded;
      updateDocumentPanel();
      try { localStorage.setItem('documents-panel', state.documentsExpanded ? 'expanded' : 'collapsed'); } catch (_) {}
    });
    uploadButton.addEventListener('click', () => uploadInput.click());
    uploadInput.addEventListener('change', () => uploadCVs(Array.from(uploadInput.files || [])));
    document.getElementById('refresh').addEventListener('click', () => loadDocuments(true));
    document.addEventListener('keydown', event => {
      const isTyping = event.target instanceof HTMLInputElement || event.target instanceof HTMLTextAreaElement;
      if (event.key === '/' && !isTyping) {
        event.preventDefault();
        search.focus();
      }
      if (event.key === 'Escape' && document.activeElement === search && search.value) {
        search.value = '';
        state.query = '';
        renderList();
        renderApplications();
      }
    });
    try { state.documentsExpanded = localStorage.getItem('documents-panel') !== 'collapsed'; } catch (_) {}
    updateDocumentPanel();
    loadDocuments();
    setInterval(loadDocuments, 10000);
  </script>
</body>
</html>
"""


def human_title(path: Path, kind: str) -> str:
    """Turn generated filenames into concise UI labels."""
    stem = path.stem
    if stem == "main_example":
        return "Master CV"
    if stem == "Candidate_Master_CV":
        return "Master CV"
    if stem == "cover_example":
        return "Example Cover Letter"
    for prefix in ("Candidate_CV_", "main_", "cover_", "Cover_"):
        if stem.startswith(prefix):
            stem = stem[len(prefix) :]
            break
    title = re.sub(r"[_-]+", " ", stem).strip().title()
    return title or DOCUMENT_DIRECTORIES[kind]


def discover_documents(repo_root: Path) -> list[dict[str, object]]:
    """Return supported documents from the two allowed document directories."""
    repo_root = repo_root.resolve()
    documents: list[dict[str, object]] = []
    for directory, label in DOCUMENT_DIRECTORIES.items():
        base = repo_root / directory
        if not base.is_dir():
            continue
        allowed = base.resolve()
        for path in base.iterdir():
            if path.suffix.lower() not in DOCUMENT_EXTENSIONS:
                continue
            if not path.is_file() or path.resolve().parent != allowed:
                continue
            stat = path.stat()
            kind = "cv" if directory == "cv" else "cover_letter"
            document_id = f"{directory}/{path.name}"
            documents.append(
                {
                    "id": document_id,
                    "title": human_title(path, directory),
                    "filename": path.name,
                    "kind": kind,
                    "kind_label": label,
                    "format": path.suffix.removeprefix(".").upper(),
                    "size": stat.st_size,
                    "modified": datetime.fromtimestamp(stat.st_mtime).astimezone().isoformat(timespec="seconds"),
                    "mtime": stat.st_mtime,
                    "url": f"/files/{quote(document_id, safe='/')}",
                }
            )
    documents.sort(key=lambda item: (float(item["mtime"]), item["kind"] == "cv"), reverse=True)
    for document in documents:
        document.pop("mtime", None)
    return documents


def save_uploaded_cv(repo_root: Path, raw_filename: object, content: bytes) -> Path:
    """Validate and atomically store an uploaded PDF or DOCX in cv/."""
    if not isinstance(raw_filename, str):
        raise ValueError("A filename is required")
    filename = raw_filename.strip()
    if not filename or len(filename) > 160:
        raise ValueError("Filename must be between 1 and 160 characters")
    if filename in {".", ".."} or "/" in filename or "\\" in filename:
        raise ValueError("Filename must not contain a path")
    if any(ord(character) < 32 for character in filename) or '"' in filename:
        raise ValueError("Filename contains unsupported characters")
    extension = Path(filename).suffix.lower()
    if extension not in DOCUMENT_EXTENSIONS:
        raise ValueError("Only PDF and DOCX files can be uploaded")
    if not content:
        raise ValueError("Uploaded file is empty")
    if len(content) > MAX_UPLOAD_BYTES:
        raise ValueError("Uploaded file exceeds the 20 MB limit")
    if extension == ".pdf" and not content.startswith(b"%PDF-"):
        raise ValueError("The uploaded file is not a valid PDF")
    if extension == ".docx":
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                names = set(archive.namelist())
                if "[Content_Types].xml" not in names or "word/document.xml" not in names:
                    raise ValueError("The uploaded file is not a valid DOCX")
        except zipfile.BadZipFile as exc:
            raise ValueError("The uploaded file is not a valid DOCX") from exc

    cv_directory = (repo_root.resolve() / "cv").resolve()
    if not cv_directory.is_dir():
        raise ValueError("CV directory is unavailable")
    destination = (cv_directory / filename).resolve()
    if destination.parent != cv_directory:
        raise ValueError("Filename must not contain a path")

    temporary_name: str | None = None
    with UPLOAD_WRITE_LOCK:
        if destination.exists():
            raise FileExistsError(f"{filename} already exists; rename the file before uploading")
        try:
            with tempfile.NamedTemporaryFile("wb", dir=cv_directory, delete=False) as temporary:
                temporary_name = temporary.name
                temporary.write(content)
                temporary.flush()
                os.fsync(temporary.fileno())
            try:
                os.link(temporary_name, destination)
            except FileExistsError as exc:
                raise FileExistsError(f"{filename} already exists; rename the file before uploading") from exc
        finally:
            if temporary_name is not None:
                Path(temporary_name).unlink(missing_ok=True)
    return destination


MATCH_TOKEN_RE = re.compile(r"[a-z0-9]+")
MATCH_NOISE = {
    "a",
    "and",
    "at",
    "co",
    "company",
    "cv",
    "sample",
    "candidate",
    "inc",
    "israel",
    "ltd",
    "main",
    "master",
    "of",
    "software",
    "technology",
    "technologies",
    "the",
}


def _match_tokens(value: object) -> set[str]:
    """Return forgiving search tokens without inventing cross-company matches."""
    tokens: set[str] = set()
    for token in MATCH_TOKEN_RE.findall(str(value or "").casefold()):
        if token in MATCH_NOISE:
            continue
        tokens.add(token)
        if len(token) > 4 and token.endswith("s"):
            tokens.add(token[:-1])
        if token in {"presale", "presales"}:
            tokens.update({"pre", "sale", "sales", "presale", "presales"})
    return tokens


def _document_filename_from_reference(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        return ""
    filename = PurePosixPath(value.strip().replace("\\", "/")).name
    if filename.lower().endswith(".tex"):
        filename = f"{filename[:-4]}.pdf"
    return filename if filename.lower().endswith(".pdf") else ""


def _attach_cv_downloads(
    applications: list[dict[str, str]], documents: list[dict[str, object]]
) -> None:
    """Attach an explicit or conservatively inferred tailored CV to each role."""
    cvs = [document for document in documents if document.get("kind") == "cv"]
    by_filename = {
        str(document.get("filename", "")).casefold(): document
        for document in cvs
        if document.get("filename")
    }

    for application in applications:
        matched: dict[str, object] | None = None
        referenced = _document_filename_from_reference(application.pop("cv_reference", ""))
        if referenced:
            matched = by_filename.get(referenced.casefold())

        if matched is None:
            company_tokens = _match_tokens(application.get("company"))
            role_tokens = _match_tokens(application.get("title"))
            best_score = -1.0
            for document in cvs:
                filename = str(document.get("filename", ""))
                if filename in {"Candidate_Master_CV.pdf", "main_example.pdf"}:
                    continue
                document_tokens = _match_tokens(Path(filename).stem)
                company_overlap = len(company_tokens & document_tokens)
                if company_overlap == 0:
                    continue
                role_overlap = len(role_tokens & document_tokens)
                role_coverage = role_overlap / max(len(role_tokens), 1)
                if role_overlap < min(2, len(role_tokens)) and role_coverage < 0.5:
                    continue
                score = company_overlap * 8 + role_overlap * 3 + role_coverage
                if score > best_score:
                    matched = document
                    best_score = score

        application["cv_url"] = str(matched.get("url", "")) if matched else ""
        application["cv_filename"] = str(matched.get("filename", "")) if matched else ""


def _safe_job_url(value: object) -> str | None:
    """Accept a well-formed HTTPS job link without embedded credentials."""
    if not isinstance(value, str) or len(value) > 2_000:
        return None
    parsed = urlsplit(value.strip())
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        return None
    return value.strip()


def _linkedin_people_search(query: str) -> str:
    params = urlencode({"keywords": query, "origin": "GLOBAL_SEARCH_HEADER"})
    return f"https://www.linkedin.com/search/results/people/?{params}"


def _normalize_status(value: object) -> str:
    if not isinstance(value, str):
        return "applied"
    normalized = re.sub(r"[\s-]+", "_", value.strip().lower())
    return normalized or "applied"


def _application_category(status: str) -> str:
    if status in REJECTED_STATUSES:
        return "rejected"
    if status in APPLIED_STATUSES:
        return "applied"
    return "updates"


def _latest_tracker_date(date_value: object, notes_value: object) -> str:
    candidates: list[str] = []
    if isinstance(date_value, str):
        candidates.extend(DATE_RE.findall(date_value))
    if isinstance(notes_value, str):
        candidates.extend(DATE_RE.findall(notes_value))
    valid: list[str] = []
    for candidate in candidates:
        try:
            datetime.strptime(candidate, "%Y-%m-%d")
        except ValueError:
            continue
        valid.append(candidate)
    return max(valid, default="")


def discover_application_links(
    repo_root: Path, documents: list[dict[str, object]] | None = None
) -> list[dict[str, str]]:
    """Build suggested and tracked role cards from the two local state files."""
    repo_root = repo_root.resolve()
    tracker_path = repo_root / "job_search_tracker.csv"
    try:
        tracker = tracker_path.open("r", encoding="utf-8-sig", newline="")
    except (FileNotFoundError, OSError, UnicodeError):
        tracker = None

    applications: list[dict[str, str]] = []
    deduplicated: set[tuple[str, str]] = set()
    tracked_urls: set[str] = set()
    if tracker is not None:
        try:
            with tracker:
                for row in csv.DictReader(tracker):
                    company = (row.get("company") or "").strip()[:200]
                    title = (row.get("role") or "").strip()[:200]
                    if not company or not title:
                        continue
                    identity = (company.casefold(), title.casefold())
                    if identity in deduplicated:
                        continue
                    deduplicated.add(identity)
                    status = _normalize_status(row.get("status"))
                    job_url = _safe_job_url(row.get("source")) or ""
                    if job_url:
                        tracked_urls.add(job_url)
                    raw_notes = (row.get("notes") or "").strip()
                    notes = raw_notes[:2_000]
                    applications.append(
                        {
                            "company": company,
                            "title": title,
                            "status": status,
                            "status_label": status.replace("_", " ").title(),
                            "category": _application_category(status),
                            "date": (row.get("date") or "").strip()[:20],
                            "last_update": _latest_tracker_date(row.get("date"), raw_notes),
                            "date_label": "Updated",
                            "channel": (row.get("channel") or "").strip()[:100],
                            "fit": (row.get("fit_rating") or "").strip().lower()[:20],
                            "notes": notes,
                            "job_url": job_url,
                            "cv_reference": (row.get("cv_file") or "").strip()[:500],
                            "recruiter_url": _linkedin_people_search(f"{company} recruiter"),
                            "team_url": _linkedin_people_search(f"{company} {title}"),
                        }
                    )
        except (csv.Error, OSError, UnicodeError):
            applications.clear()
            deduplicated.clear()

    state_path = repo_root / "job_scraper" / "seen_jobs.json"
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError):
        payload = {}
    seen = payload.get("seen") if isinstance(payload, dict) else None
    if isinstance(seen, dict):
        for job in seen.values():
            if not isinstance(job, dict):
                continue
            if job.get("status") not in {"new", "ranked", "evaluated"}:
                continue
            fit = str(job.get("fit") or "").strip().lower()
            if fit not in {"high", "medium"}:
                continue
            company = str(job.get("company") or "").strip()[:200]
            title = str(job.get("title") or "").strip()[:200]
            job_url = _safe_job_url(job.get("url"))
            if not company or not title or job_url is None:
                continue
            if job_url in tracked_urls:
                continue
            identity = (company.casefold(), title.casefold())
            if identity in deduplicated:
                continue
            deduplicated.add(identity)
            first_seen = str(job.get("first_seen") or "").strip()[:20]
            rank_verdict = str(job.get("rank_verdict") or "").strip()[:100]
            notes = rank_verdict if rank_verdict else "Not yet applied."
            applications.append(
                {
                    "company": company,
                    "title": title,
                    "status": "suggested",
                    "status_label": "Suggested",
                    "category": "suggested",
                    "date": first_seen,
                    "last_update": _latest_tracker_date(first_seen, ""),
                    "date_label": "Added",
                    "channel": "Job lead",
                    "fit": fit,
                    "notes": notes,
                    "job_url": job_url,
                    "cv_reference": str(job.get("cv_file") or "").strip()[:500],
                    "recruiter_url": _linkedin_people_search(f"{company} recruiter"),
                    "team_url": _linkedin_people_search(f"{company} {title}"),
                }
            )

    applications.sort(key=lambda item: (item["company"].casefold(), item["title"].casefold()))
    applications.sort(key=lambda item: item["last_update"], reverse=True)
    category_order = {"suggested": 0, "updates": 1, "applied": 2, "rejected": 3}
    applications.sort(key=lambda item: category_order[item["category"]])
    _attach_cv_downloads(applications, documents or discover_documents(repo_root))
    return applications


def mark_suggested_job_applied(repo_root: Path, job_url: object, today: str | None = None) -> dict[str, str]:
    """Append a suggested role to the private application tracker exactly once."""
    repo_root = repo_root.resolve()
    safe_url = _safe_job_url(job_url)
    if safe_url is None:
        raise ValueError("A valid HTTPS job URL is required")

    state_path = repo_root / "job_scraper" / "seen_jobs.json"
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LookupError("Suggested job state is unavailable") from exc
    seen = payload.get("seen") if isinstance(payload, dict) else None
    job = seen.get(safe_url) if isinstance(seen, dict) else None
    if not isinstance(job, dict) or job.get("status") not in {"new", "ranked", "evaluated"}:
        raise LookupError("Suggested job was not found")
    fit = str(job.get("fit") or "").strip().lower()
    company = str(job.get("company") or "").strip()[:200]
    title = str(job.get("title") or "").strip()[:200]
    canonical_url = _safe_job_url(job.get("url"))
    if fit not in {"high", "medium"} or not company or not title or canonical_url != safe_url:
        raise LookupError("Suggested job was not found")

    tracker_path = repo_root / "job_search_tracker.csv"
    with TRACKER_WRITE_LOCK:
        rows: list[dict[str, str]] = []
        if tracker_path.exists():
            try:
                with tracker_path.open("r", encoding="utf-8-sig", newline="") as source:
                    reader = csv.DictReader(source)
                    if reader.fieldnames != list(TRACKER_FIELDS):
                        raise ValueError("Application tracker has an unexpected header")
                    rows = [{field: row.get(field, "") for field in TRACKER_FIELDS} for row in reader]
            except (csv.Error, OSError, UnicodeError) as exc:
                raise ValueError("Application tracker could not be read") from exc

        identity = (company.casefold(), title.casefold())
        for row in rows:
            existing_url = _safe_job_url(row.get("source"))
            existing_identity = (
                (row.get("company") or "").strip().casefold(),
                (row.get("role") or "").strip().casefold(),
            )
            if existing_url == safe_url or existing_identity == identity:
                return {"status": "already_applied", "company": company, "title": title}

        applied_date = today or datetime.now().astimezone().date().isoformat()
        try:
            datetime.strptime(applied_date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("Application date must use YYYY-MM-DD") from exc
        rows.append(
            {
                "date": applied_date,
                "company": company,
                "sector": "",
                "role": title,
                "role_type": "",
                "channel": "online",
                "status": "applied",
                "contact_person": "",
                "fit_rating": fit,
                "notes": f"{applied_date}: Marked applied in private viewer.",
                "cv_file": "",
                "cover_letter_file": "",
                "source": safe_url,
            }
        )

        tracker_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_name: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", newline="", dir=tracker_path.parent, delete=False
            ) as destination:
                temporary_name = destination.name
                writer = csv.DictWriter(destination, fieldnames=TRACKER_FIELDS, lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)
                destination.flush()
                os.fsync(destination.fileno())
            os.replace(temporary_name, tracker_path)
        finally:
            if temporary_name is not None:
                try:
                    Path(temporary_name).unlink(missing_ok=True)
                except OSError:
                    pass
    return {"status": "applied", "company": company, "title": title}


def resolve_document_path(repo_root: Path, raw_path: str) -> Path | None:
    """Resolve a URL document id without allowing traversal or arbitrary reads."""
    decoded = unquote(raw_path)
    relative = PurePosixPath(decoded)
    if relative.is_absolute() or len(relative.parts) < 2 or '..' in relative.parts or '\\' in decoded or '\x00' in decoded:
        return None
    directory, filename = relative.parts[0], relative.name
    if directory not in DOCUMENT_DIRECTORIES or Path(filename).suffix.lower() not in DOCUMENT_EXTENSIONS:
        return None
    allowed = (repo_root.resolve() / directory).resolve()
    candidate = repo_root.resolve().joinpath(*relative.parts).resolve()
    if not candidate.is_relative_to(allowed) or not candidate.is_file():
        return None
    return candidate


def is_loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class ViewerHandler(BaseHTTPRequestHandler):
    server_version = "JobPortal/2.0"

    @property
    def repo_root(self) -> Path:
        return self.server.repo_root  # type: ignore[attr-defined]

    @property
    def portal_store(self) -> PortalStore:
        return self.server.portal_store  # type: ignore[attr-defined]

    @property
    def portal_worker(self) -> PortalWorker:
        return self.server.portal_worker  # type: ignore[attr-defined]

    @property
    def auth_token(self) -> str:
        return self.server.auth_token  # type: ignore[attr-defined]

    def _headers(self, content_type: str, content_length: int | None = None) -> None:
        self.send_header("Content-Type", content_type)
        if content_length is not None:
            self.send_header("Content-Length", str(content_length))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "SAMEORIGIN")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; "
            "frame-src 'self'; object-src 'self'; base-uri 'none'; form-action 'none'",
        )

    def _send_bytes(
        self,
        body: bytes,
        content_type: str,
        status: HTTPStatus = HTTPStatus.OK,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self._headers(content_type, len(body))
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_json(
        self,
        payload: object,
        status: HTTPStatus = HTTPStatus.OK,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._send_bytes(body, "application/json; charset=utf-8", status, extra_headers)

    def _session_value(self) -> str:
        return hashlib.sha256(f"job-portal-session:{self.auth_token}".encode()).hexdigest()

    def _authenticated(self) -> bool:
        if not self.auth_token:
            return True
        authorization = self.headers.get("Authorization", "")
        if authorization.startswith("Bearer ") and hmac.compare_digest(
            authorization[7:], self.auth_token
        ):
            return True
        cookie = SimpleCookie()
        try:
            cookie.load(self.headers.get("Cookie", ""))
        except Exception:
            return False
        value = cookie.get("portal_session")
        return bool(value and hmac.compare_digest(value.value, self._session_value()))

    def _require_auth(self, *, write: bool = False) -> bool:
        if not self._authenticated():
            self._send_json({"error": "Authentication required"}, HTTPStatus.UNAUTHORIZED)
            return False
        if write and self.auth_token and self.headers.get("X-Requested-With") != "job-portal":
            self._send_json({"error": "Missing request verification header"}, HTTPStatus.FORBIDDEN)
            return False
        return True

    def _read_json(self, maximum: int = 100_000) -> dict[str, object]:
        if not self.headers.get("Content-Type", "").lower().startswith("application/json"):
            raise ValidationError("Content-Type must be application/json")
        try:
            content_length = int(self.headers.get("Content-Length", ""))
        except ValueError as exc:
            raise ValidationError("Invalid request size") from exc
        if content_length < 0 or content_length > maximum:
            raise ValidationError("Invalid request size")
        try:
            payload = json.loads(self.rfile.read(content_length) or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValidationError("Invalid JSON") from exc
        if not isinstance(payload, dict):
            raise ValidationError("JSON object required")
        return payload

    def _handle_portal_error(self, exc: Exception) -> None:
        if isinstance(exc, NotFoundError):
            self._send_json({"error": str(exc)}, HTTPStatus.NOT_FOUND)
        elif isinstance(exc, ConflictError):
            self._send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
        elif isinstance(exc, (ValidationError, ValueError)):
            self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
        elif isinstance(exc, (CodexUnavailable, CodexRPCError)):
            self._send_json({"error": str(exc)}, HTTPStatus.SERVICE_UNAVAILABLE)
        else:
            self.log_error("Portal request failed: %r", exc)
            self._send_json({"error": "Portal operation failed"}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def _serve_document(self, raw_path: str, download: bool) -> None:
        path = resolve_document_path(self.repo_root, raw_path)
        if path is None:
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        size = path.stat().st_size
        start, end = 0, size - 1
        status = HTTPStatus.OK
        range_header = self.headers.get("Range")
        if range_header:
            match = RANGE_RE.fullmatch(range_header.strip())
            if not match:
                self.send_error(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                return
            first, last = match.groups()
            if not first and not last:
                self.send_error(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                return
            if first:
                start = int(first)
                end = min(int(last), size - 1) if last else size - 1
            else:
                suffix = int(last)
                start = max(size - suffix, 0)
            if start >= size or end < start:
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.send_header("Content-Range", f"bytes */{size}")
                self.end_headers()
                return
            status = HTTPStatus.PARTIAL_CONTENT

        length = end - start + 1
        self.send_response(status)
        content_type = mimetypes.types_map.get(path.suffix.lower(), "application/octet-stream")
        self._headers(content_type, length)
        self.send_header("Accept-Ranges", "bytes")
        if status == HTTPStatus.PARTIAL_CONTENT:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        disposition = "inline" if path.suffix.lower() == ".pdf" and not download else "attachment"
        ascii_filename = re.sub(r"[^A-Za-z0-9._-]", "_", path.name) or "document"
        encoded_filename = quote(path.name, safe="")
        self.send_header(
            "Content-Disposition",
            f'{disposition}; filename="{ascii_filename}"; filename*=UTF-8\'\'{encoded_filename}',
        )
        self.end_headers()
        if self.command == "HEAD":
            return
        with path.open("rb") as source:
            source.seek(start)
            remaining = length
            while remaining:
                chunk = source.read(min(64 * 1024, remaining))
                if not chunk:
                    break
                self.wfile.write(chunk)
                remaining -= len(chunk)

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        parsed = urlsplit(self.path)
        if parsed.path == "/":
            self._send_bytes(PORTAL_HTML.encode("utf-8"), "text/html; charset=utf-8")
            return
        if parsed.path == "/healthz":
            self._send_json({"status": "ok", "worker": "running"})
            return
        if parsed.path == "/api/session":
            if not self._authenticated():
                self._send_json({"error": "Authentication required"}, HTTPStatus.UNAUTHORIZED)
            else:
                self._send_json({"authenticated": True, "auth_enabled": bool(self.auth_token)})
            return
        if not self._require_auth():
            return
        try:
            if parsed.path == "/api/documents":
                documents = discover_documents(self.repo_root)
                self._send_json(
                    {
                        "documents": documents,
                        "applications": discover_application_links(self.repo_root, documents),
                    }
                )
                return
            if parsed.path == "/api/jobs":
                query = parse_qs(parsed.query)
                statuses = query.get("status") or None
                jobs = self.portal_store.list_jobs(
                    query=(query.get("q") or [""])[0],
                    statuses=statuses,
                    include_archived=(query.get("include_archived") or [""])[0] == "1",
                    include_discoveries=(query.get("include_discoveries") or [""])[0] == "1",
                    listing_status=(query.get("listing_status") or [None])[0],
                )
                self._send_json({"jobs": jobs})
                return
            job_match = re.fullmatch(r"/api/jobs/(job_[a-f0-9]{20})", parsed.path)
            if job_match:
                self._send_json(self.portal_store.get_job(job_match.group(1)))
                return
            if parsed.path == "/api/runs":
                query = parse_qs(parsed.query)
                self._send_json({
                    "runs": self.portal_store.list_runs(
                        limit=500,
                        include_archived=(query.get("include_archived") or [""])[0] == "1",
                    ),
                    "history": self.portal_store.run_history_counts(),
                })
                return
            stop_match = re.fullmatch(r"/api/stops/(stop_[a-f0-9]{32})", parsed.path)
            if stop_match:
                self._send_json(self.portal_store.get_stop(stop_match.group(1)))
                return
            run_match = re.fullmatch(r"/api/runs/(run_[a-f0-9]{32})", parsed.path)
            if run_match:
                self._send_json(self.portal_store.get_run(run_match.group(1)))
                return
            event_match = re.fullmatch(r"/api/runs/(run_[a-f0-9]{32})/events", parsed.path)
            if event_match:
                self._serve_run_events(event_match.group(1), parsed.query)
                return
            if parsed.path == "/api/conversations":
                self._send_json({"conversations": self.portal_store.list_conversations()})
                return
            conversation_match = re.fullmatch(r"/api/conversations/(conv_[a-f0-9]{32})", parsed.path)
            if conversation_match:
                self._send_json(self.portal_store.get_conversation(conversation_match.group(1)))
                return
            if parsed.path == "/api/codex/threads":
                threads = self.portal_worker.codex.list_threads(
                    cwd=[str(self.repo_root), str(self.repo_root.parent)], limit=50
                )
                self._send_json(
                    {
                        "threads": [
                            {
                                "id": item.get("id"),
                                "preview": item.get("preview"),
                                "cwd": item.get("cwd"),
                                "source": item.get("source"),
                                "updated_at": item.get("updatedAt"),
                            }
                            for item in threads
                        ]
                    }
                )
                return
            if parsed.path.startswith("/files/"):
                download = parsed.query == "download=1"
                self._serve_document(parsed.path[len("/files/") :], download)
                return
            self.send_error(HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self._handle_portal_error(exc)

    def _serve_run_events(self, run_id: str, query_string: str) -> None:
        query = parse_qs(query_string)
        try:
            after = int((query.get("after") or [self.headers.get("Last-Event-ID", "0")])[0] or 0)
        except ValueError:
            after = 0
        self.send_response(HTTPStatus.OK)
        self._headers("text/event-stream; charset=utf-8")
        # This bounded response has no Content-Length/chunked terminator. Keep-alive
        # would leave EventSource waiting forever after the handler's deadline.
        self.send_header("Connection", "close")
        self.close_connection = True
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        deadline = time.monotonic() + getattr(self, "run_stream_seconds", 25)
        last_id = after
        try:
            while time.monotonic() < deadline:
                events = self.portal_store.run_events(run_id, last_id)
                for event in events:
                    last_id = int(event["id"])
                    payload = json.dumps({"event": event, "terminal": False}, ensure_ascii=False)
                    self.wfile.write(f"id: {last_id}\ndata: {payload}\n\n".encode())
                run = self.portal_store.get_run(run_id)
                terminal = run["state"] in {"completed", "failed", "cancelled", "interrupted"}
                if terminal:
                    payload = json.dumps(
                        {"terminal": True, "state": run["state"], "run": run}, ensure_ascii=False
                    )
                    self.wfile.write(f"data: {payload}\n\n".encode())
                    self.wfile.flush()
                    return
                if events:
                    self.wfile.flush()
                else:
                    self.wfile.write(b": heartbeat\n\n")
                    self.wfile.flush()
                time.sleep(0.75)
        except (BrokenPipeError, ConnectionResetError):
            return

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib handler API
        self.do_GET()

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        parsed = urlsplit(self.path)
        if parsed.path == "/api/auth/login":
            self._login()
            return
        if not self._require_auth(write=True):
            return
        if parsed.path == "/api/auth/logout":
            self._send_json(
                {"status": "signed_out"},
                extra_headers={
                    "Set-Cookie": "portal_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict"
                },
            )
            return
        if parsed.path == "/api/cv/upload":
            self._upload_cv()
            return
        if parsed.path == "/api/applications/mark-applied":
            self._mark_applied()
            return
        try:
            body = self._read_json()
            if parsed.path == "/api/runs/archive-history":
                self._send_json(self.portal_store.archive_run_history(
                    source="portal", idempotency_key=body.get("idempotency_key"),
                ))
                return
            if parsed.path == "/api/searches":
                run = self.portal_store.enqueue_run(
                    "search",
                    payload={"focus": str(body.get("focus") or "")[:500]},
                    idempotency_key=str(body.get("idempotency_key") or "") or None,
                )
                self.portal_worker.wake()
                self._send_json({"run": run}, HTTPStatus.ACCEPTED)
                return
            if parsed.path == '/api/cvs/generate-new':
                result = self.portal_store.enqueue_new_cvs(idempotency_key=body.get('idempotency_key'))
                limit = self.portal_worker.cv_concurrency
                result['concurrency'] = limit if type(limit) is int else None
                self.portal_worker.wake()
                self._send_json(result, HTTPStatus.ACCEPTED)
                return
            if parsed.path == "/api/listings/refresh-eligible":
                run = self.portal_store.enqueue_refresh_batch(
                    body,
                    idempotency_key=str(body.get("idempotency_key") or "") or None,
                )
                self.portal_worker.wake()
                self._send_json({"run": run}, HTTPStatus.ACCEPTED)
                return
            if parsed.path == "/api/listings/refresh-preview":
                self._send_json(self.portal_store.preview_refresh_batch(body))
                return
            stop_batch = re.fullmatch(r"/api/runs/(run_[a-f0-9]{32})/stop-batch", parsed.path)
            if parsed.path == "/api/runs/stop-all" or stop_batch:
                self._send_json(self.portal_worker.stop_runs(
                    batch_id=stop_batch.group(1) if stop_batch else None,
                    idempotency_key=body.get("idempotency_key"),
                ), HTTPStatus.ACCEPTED)
                return
            status_match = re.fullmatch(r"/api/jobs/(job_[a-f0-9]{20})/status", parsed.path)
            if status_match:
                expected = body.get("expected_version")
                job = self.portal_store.update_application_status(
                    status_match.group(1),
                    str(body.get("status") or ""),
                    source="portal",
                    note=str(body.get("note") or "")[:4000],
                    idempotency_key=str(body.get("idempotency_key") or "") or None,
                    expected_version=int(expected) if isinstance(expected, int) else None,
                )
                self._send_json({"job": job})
                return
            note_match = re.fullmatch(r"/api/jobs/(job_[a-f0-9]{20})/notes", parsed.path)
            if note_match:
                job = self.portal_store.add_note(
                    note_match.group(1),
                    str(body.get("note") or ""),
                    source="portal",
                    idempotency_key=str(body.get("idempotency_key") or "") or None,
                )
                self._send_json({"job": job}, HTTPStatus.CREATED)
                return
            task_match = re.fullmatch(
                r"/api/jobs/(job_[a-f0-9]{20})/(refresh|evaluate|generate-cv)", parsed.path
            )
            if task_match:
                task = {
                    "refresh": "refresh_listing",
                    "evaluate": "evaluate",
                    "generate-cv": "generate_cv",
                }[task_match.group(2)]
                run = self.portal_store.enqueue_run(
                    task,
                    job_id=task_match.group(1),
                    payload={},
                    idempotency_key=str(body.get("idempotency_key") or "") or None,
                )
                self.portal_worker.wake()
                self._send_json({"run": run}, HTTPStatus.ACCEPTED)
                return
            cancel_match = re.fullmatch(r"/api/runs/(run_[a-f0-9]{32})/cancel", parsed.path)
            if cancel_match:
                self._send_json({"run": self.portal_worker.cancel(cancel_match.group(1))})
                return
            retry_match = re.fullmatch(r"/api/runs/(run_[a-f0-9]{32})/retry", parsed.path)
            if retry_match:
                run = self.portal_store.retry_run(
                    retry_match.group(1), str(body.get("idempotency_key") or "") or None
                )
                self.portal_worker.wake()
                self._send_json({"run": run}, HTTPStatus.ACCEPTED)
                return
            approval_match = re.fullmatch(r"/api/approvals/(approval_[a-f0-9]{32})", parsed.path)
            if approval_match:
                approval = self.portal_worker.resolve_approval(
                    approval_match.group(1), str(body.get("decision") or "")
                )
                self._send_json({"approval": approval})
                return
            if parsed.path == "/api/conversations":
                conversation = self.portal_store.create_conversation(
                    str(body.get("title") or "New conversation"),
                    job_id=str(body.get("job_id") or "") or None,
                )
                self._send_json({"conversation": conversation}, HTTPStatus.CREATED)
                return
            if parsed.path == "/api/conversations/import":
                conversation = self._import_conversation(
                    str(body.get("thread_id") or ""),
                    str(body.get("title") or "Imported Codex conversation"),
                    str(body.get("job_id") or "") or None,
                )
                self._send_json({"conversation": conversation}, HTTPStatus.CREATED)
                return
            message_match = re.fullmatch(
                r"/api/conversations/(conv_[a-f0-9]{32})/messages", parsed.path
            )
            if message_match:
                conversation_id = message_match.group(1)
                message = str(body.get("message") or "").strip()
                if not message:
                    raise ValidationError("Message cannot be empty")
                run = self.portal_store.enqueue_run(
                    "chat",
                    conversation_id=conversation_id,
                    job_id=self.portal_store.get_conversation(conversation_id).get("job_id"),
                    payload={"message": message},
                    idempotency_key=str(body.get("idempotency_key") or "") or None,
                )
                self.portal_store.add_message(conversation_id, "user", message, run_id=run["id"])
                self.portal_worker.wake()
                self._send_json({"run": run}, HTTPStatus.ACCEPTED)
                return
            self.send_error(HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self._handle_portal_error(exc)

    def _login(self) -> None:
        try:
            body = self._read_json(10_000)
        except Exception as exc:
            self._handle_portal_error(exc)
            return
        supplied = str(body.get("token") or "")
        if self.auth_token and not hmac.compare_digest(supplied, self.auth_token):
            self._send_json({"error": "Invalid portal token"}, HTTPStatus.UNAUTHORIZED)
            return
        attributes = [
            f"portal_session={self._session_value()}",
            "Path=/",
            "HttpOnly",
            "SameSite=Strict",
            "Max-Age=2592000",
        ]
        if self.server.cookie_secure:  # type: ignore[attr-defined]
            attributes.append("Secure")
        self._send_json(
            {"authenticated": True, "auth_enabled": bool(self.auth_token)},
            extra_headers={"Set-Cookie": "; ".join(attributes)},
        )

    def _import_conversation(
        self, thread_id: str, title: str, job_id: str | None
    ) -> dict[str, object]:
        if not re.fullmatch(r"[0-9a-f-]{20,80}", thread_id):
            raise ValidationError("A valid Codex thread ID is required")
        thread = self.portal_worker.codex.read_thread(thread_id, include_turns=True)
        if not thread:
            raise NotFoundError("Codex thread was not found")
        conversation = self.portal_store.create_conversation(
            title or str(thread.get("preview") or "Imported Codex conversation"),
            job_id=job_id,
            codex_thread_id=thread_id,
            source="imported_cli",
        )
        current = self.portal_store.get_conversation(str(conversation["id"]))
        if not current["messages"]:
            for turn in thread.get("turns") or []:
                if not isinstance(turn, dict):
                    continue
                for item in turn.get("items") or []:
                    if not isinstance(item, dict):
                        continue
                    if item.get("type") == "userMessage":
                        parts = [
                            str(part.get("text") or "")
                            for part in item.get("content") or []
                            if isinstance(part, dict) and part.get("type") == "text"
                        ]
                        text = "\n".join(filter(None, parts)).strip()
                        if text:
                            self.portal_store.add_message(str(conversation["id"]), "user", text)
                    elif item.get("type") == "agentMessage" and str(item.get("text") or "").strip():
                        self.portal_store.add_message(
                            str(conversation["id"]), "assistant", str(item["text"])
                        )
        return self.portal_store.get_conversation(str(conversation["id"]))

    def _upload_cv(self) -> None:
        try:
            content_length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            content_length = -1
        if content_length < 1:
            self._send_json({"error": "Uploaded file is empty"}, HTTPStatus.BAD_REQUEST)
            return
        if content_length > MAX_UPLOAD_BYTES:
            self._send_json({"error": "Uploaded file exceeds the 20 MB limit"}, HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
            return
        encoded_filename = self.headers.get("X-Filename", "")
        if len(encoded_filename) > 500:
            self._send_json({"error": "Filename is too long"}, HTTPStatus.BAD_REQUEST)
            return
        filename = unquote(encoded_filename)
        content = self.rfile.read(content_length)
        if len(content) != content_length:
            self._send_json({"error": "Upload was incomplete"}, HTTPStatus.BAD_REQUEST)
            return
        try:
            destination = save_uploaded_cv(self.repo_root, filename, content)
        except FileExistsError as exc:
            self._send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
            return
        except ValueError as exc:
            self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # Keep handler failures from becoming an empty HTTP response.
            self.log_error("Could not save uploaded CV: %r", exc)
            self._send_json({"error": "Could not save uploaded CV"}, HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self._send_json(
            {"status": "uploaded", "filename": destination.name, "folder": "cv"},
            HTTPStatus.CREATED,
        )

    def _mark_applied(self) -> None:
        if not self.headers.get("Content-Type", "").lower().startswith("application/json"):
            self._send_json({"error": "Content-Type must be application/json"}, HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
            return
        try:
            content_length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            content_length = -1
        if content_length < 1 or content_length > 4_096:
            self._send_json({"error": "Invalid request size"}, HTTPStatus.BAD_REQUEST)
            return
        try:
            body = json.loads(self.rfile.read(content_length))
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._send_json({"error": "Invalid JSON"}, HTTPStatus.BAD_REQUEST)
            return
        if not isinstance(body, dict):
            self._send_json({"error": "JSON object required"}, HTTPStatus.BAD_REQUEST)
            return
        try:
            result = mark_suggested_job_applied(self.repo_root, body.get("job_url"))
        except LookupError as exc:
            self._send_json({"error": str(exc)}, HTTPStatus.NOT_FOUND)
            return
        except ValueError as exc:
            self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:  # Keep handler failures from becoming an empty HTTP response.
            self.log_error("Could not update application tracker: %r", exc)
            self._send_json(
                {"error": "Could not update application tracker"},
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return
        self.portal_store.sync_legacy(force=True)
        self._send_json(result)

    def log_message(self, message: str, *args: object) -> None:
        sys.stderr.write(f"[{self.log_date_time_string()}] {self.address_string()} {message % args}\n")


class PortalHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def server_close(self) -> None:
        worker = getattr(self, "portal_worker", None)
        if worker is not None:
            worker.stop()
        super().server_close()


def create_server(
    repo_root: Path,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    *,
    store: PortalStore | None = None,
    worker: PortalWorker | None = None,
) -> ThreadingHTTPServer:
    repo_root = repo_root.resolve()
    if not (repo_root / "cv").is_dir():
        raise ValueError(f"Not an ai-job-search checkout: {repo_root}")
    if not is_loopback_host(host):
        raise ValueError("Refusing a non-loopback host; use an SSH tunnel to keep documents private")
    portal_store = store or PortalStore(repo_root, reconcile=True)
    portal_worker = worker or PortalWorker(portal_store, repo_root)
    server = PortalHTTPServer((host, port), ViewerHandler)
    server.repo_root = repo_root  # type: ignore[attr-defined]
    server.portal_store = portal_store  # type: ignore[attr-defined]
    server.portal_worker = portal_worker  # type: ignore[attr-defined]
    server.auth_token = os.environ.get("PORTAL_AUTH_TOKEN", "")  # type: ignore[attr-defined]
    server.cookie_secure = os.environ.get("PORTAL_COOKIE_SECURE", "0") == "1"  # type: ignore[attr-defined]
    portal_worker.start()
    return server


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    default_repo = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Serve generated CVs in a private local web viewer")
    parser.add_argument("--repo", type=Path, default=default_repo, help="ai-job-search repository root")
    parser.add_argument("--host", default=DEFAULT_HOST, help="loopback host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="TCP port (default: 8765)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        server = create_server(args.repo, args.host, args.port)
    except (OSError, ValueError) as exc:
        print(f"cv_webview: {exc}", file=sys.stderr)
        return 2

    print(f"CV web viewer: http://{args.host}:{server.server_port}")
    print(f"Repository: {args.repo.resolve()}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping CV web viewer.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
