# CLAUDE.md

@AGENTS.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`pyEDIutils` is a Python library for interacting with EDI's PASTA+ data repository servers. It wraps the PASTA REST API and converts XML responses into pandas DataFrames. The default scope throughout is `knb-lter-jrn` (Jornada Basin LTER). The EDI repository two separately documented service APIs, a data package manager API (`https://pasta.lternet.edu/package/docs/api`) and an audit manager API (`https://pasta.lternet.edu/audit/docs/api`). Both are accessed by functions in this library, so use both documentation sets to inform code being written.

## Architecture

The codebase is organized in two layers:

**Low-level API layer** — `pasta_api_requests.py`
- Makes raw HTTP requests to PASTA endpoints using the `requests` library
- Returns raw `requests.Response` objects or `xml.etree.ElementTree` root objects
- Handles both production (`https://pasta.lternet.edu`) and staging (`https://pasta-s.lternet.edu`) environments

**High-level feature modules** — import from `pasta_api_requests` and parse responses into DataFrames:
- `changes.py` — PASTA change records (package create/update/delete events); supports archiving XML to disk and loading from archives
- `search.py` — Apache Solr-based package search across fields like scope, author, title, packageid
- `audit_rpts.py` — Access audit reports and counts; authenticated endpoints require `dn`/`pw` credentials
- `pkginfo.py` — Builds entity metadata tables for a data package (scope/identifier/revision)

**EDI package identifier format:** `scope.identifier.revision` (e.g., `knb-lter-jrn.210.1`)

## Credentials

Authenticated PASTA API calls (audit report/count endpoints) require EDI credentials stored in `edicred.py`. A template is provided at `edicred.py.template` — copy it to `edicred.py` and fill in actual values.

`edicred.py` is intentionally untracked (do not commit it). Ensure it is never staged or committed.

## Common API Endpoints Used

| Endpoint | Module | Purpose |
|---|---|---|
| `/package/search/eml` | `search.py` | Solr package search |
| `/package/changes/eml` | `changes.py` | Recent create/update/delete events |
| `/package/name/eml/` | `pasta_api_requests.py` | Entity names/IDs for a package |
| `/package/data/rmd/eml/` | `pasta_api_requests.py` | Entity resource metadata |
| `/package/eml/` | `pasta_api_requests.py` | Package revision list |
| `/audit/reads/` | `audit_rpts.py` | Read counts by document or package |
| `/audit/report/` | `audit_rpts.py` | Detailed access log (authenticated) |
| `/audit/count/` | `audit_rpts.py` | Aggregate access counts (authenticated) |

## Running and Testing

There is no test suite or build system. The library is used interactively (e.g., in Jupyter notebooks). To exercise functions manually:

```python
import pyEDIutils.changes as ch
import pyEDIutils.search as sr
import pyEDIutils.audit_rpts as aud
import pyEDIutils.pkginfo as pkg

# Example: search packages
df = sr.search_pasta(query='scope:knb-lter-jrn', fields=['packageid','title','pubdate'])

# Example: get change records
df = ch.request_changes(fromdt='2023-01-01', todt='2023-12-31')
```

Install dependencies:
```bash
pip install requests pandas
```
