"""Integration-style tests for the REST API routes using the in-memory DB."""

from unittest.mock import patch

import pytest

from app.schemas.company import GoogleSearchResult, ZefixSearchResult


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# Companies CRUD
# ---------------------------------------------------------------------------


def test_create_and_get_company(client):
    payload = {"uid": "CHE-123.456.789", "name": "Test AG", "canton": "ZH"}
    resp = client.post("/api/v1/companies", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["uid"] == "CHE-123.456.789"
    assert data["name"] == "Test AG"

    company_id = data["id"]
    resp2 = client.get(f"/api/v1/companies/{company_id}")
    assert resp2.status_code == 200
    assert resp2.json()["name"] == "Test AG"


def test_create_duplicate_uid_returns_409(client):
    payload = {"uid": "CHE-111.222.333", "name": "Dupe AG"}
    client.post("/api/v1/companies", json=payload)
    resp = client.post("/api/v1/companies", json=payload)
    assert resp.status_code == 409


def test_list_companies(client):
    client.post("/api/v1/companies", json={"uid": "CHE-001.001.001", "name": "Alpha AG"})
    client.post("/api/v1/companies", json={"uid": "CHE-002.002.002", "name": "Beta GmbH"})
    resp = client.get("/api/v1/companies")
    assert resp.status_code == 200
    names = [c["name"] for c in resp.json()]
    assert "Alpha AG" in names
    assert "Beta GmbH" in names


def test_list_companies_name_filter(client):
    client.post("/api/v1/companies", json={"uid": "CHE-001.001.001", "name": "Alpha AG"})
    client.post("/api/v1/companies", json={"uid": "CHE-002.002.002", "name": "Beta GmbH"})
    resp = client.get("/api/v1/companies?name=alpha")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["name"] == "Alpha AG"


def test_update_company(client):
    resp = client.post("/api/v1/companies", json={"uid": "CHE-999.999.999", "name": "Old Name"})
    company_id = resp.json()["id"]
    resp2 = client.patch(f"/api/v1/companies/{company_id}", json={"name": "New Name"})
    assert resp2.status_code == 200
    assert resp2.json()["name"] == "New Name"


def test_delete_company(client):
    resp = client.post("/api/v1/companies", json={"uid": "CHE-777.777.777", "name": "To Delete"})
    company_id = resp.json()["id"]
    resp2 = client.delete(f"/api/v1/companies/{company_id}")
    assert resp2.status_code == 204
    resp3 = client.get(f"/api/v1/companies/{company_id}")
    assert resp3.status_code == 404


def test_get_nonexistent_company_returns_404(client):
    resp = client.get("/api/v1/companies/9999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Notes CRUD
# ---------------------------------------------------------------------------


def _create_company(client, uid="CHE-100.200.300", name="Note Test AG"):
    resp = client.post("/api/v1/companies", json={"uid": uid, "name": name})
    return resp.json()["id"]


def test_create_and_list_notes(client):
    company_id = _create_company(client)
    resp = client.post(f"/api/v1/companies/{company_id}/notes", json={"content": "First note"})
    assert resp.status_code == 201
    assert resp.json()["content"] == "First note"

    resp2 = client.get(f"/api/v1/companies/{company_id}/notes")
    assert resp2.status_code == 200
    assert len(resp2.json()) == 1


def test_update_note(client):
    company_id = _create_company(client)
    note_resp = client.post(f"/api/v1/companies/{company_id}/notes", json={"content": "Original"})
    note_id = note_resp.json()["id"]
    resp = client.patch(f"/api/v1/companies/{company_id}/notes/{note_id}", json={"content": "Updated"})
    assert resp.status_code == 200
    assert resp.json()["content"] == "Updated"


def test_delete_note(client):
    company_id = _create_company(client)
    note_resp = client.post(f"/api/v1/companies/{company_id}/notes", json={"content": "To delete"})
    note_id = note_resp.json()["id"]
    resp = client.delete(f"/api/v1/companies/{company_id}/notes/{note_id}")
    assert resp.status_code == 204
    resp2 = client.get(f"/api/v1/companies/{company_id}/notes/{note_id}")
    assert resp2.status_code == 404


def test_note_not_found_for_wrong_company(client):
    cid1 = _create_company(client, uid="CHE-001.001.001", name="Company 1")
    cid2 = _create_company(client, uid="CHE-002.002.002", name="Company 2")
    note_resp = client.post(f"/api/v1/companies/{cid1}/notes", json={"content": "C1 note"})
    note_id = note_resp.json()["id"]
    # Accessing the note via a different company should return 404
    resp = client.get(f"/api/v1/companies/{cid2}/notes/{note_id}")
    assert resp.status_code == 404


def test_notes_deleted_with_company(client):
    company_id = _create_company(client)
    client.post(f"/api/v1/companies/{company_id}/notes", json={"content": "Orphan note"})
    client.delete(f"/api/v1/companies/{company_id}")
    # After company deletion the notes endpoint should 404
    resp = client.get(f"/api/v1/companies/{company_id}/notes")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Zefix API search (mocked)
# ---------------------------------------------------------------------------


def test_zefix_search_route(client):
    mock_results = [ZefixSearchResult(uid="CHE-123.456.789", name="Mocked AG")]
    with patch("app.api.routes.companies.zefix_client.search_companies", return_value=mock_results):
        resp = client.get("/api/v1/companies/zefix/search?name=Mocked")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["name"] == "Mocked AG"


def test_zefix_search_propagates_error(client):
    with patch(
        "app.api.routes.companies.zefix_client.search_companies",
        side_effect=Exception("network error"),
    ):
        resp = client.get("/api/v1/companies/zefix/search?name=Fail")
    assert resp.status_code == 502


# ---------------------------------------------------------------------------
# Google Search route (mocked)
# ---------------------------------------------------------------------------


def test_google_search_route(client):
    company_id = _create_company(client)
    mock_results = [GoogleSearchResult(title="Test AG", link="https://test-ag.ch")]
    with patch("app.api.routes.companies.google_search_client.search_website", return_value=mock_results):
        resp = client.get(f"/api/v1/companies/{company_id}/google-search")
    assert resp.status_code == 200
    data = resp.json()
    assert data[0]["link"] == "https://test-ag.ch"

    # Website URL should be saved on the company
    company_resp = client.get(f"/api/v1/companies/{company_id}")
    assert company_resp.json()["website_url"] == "https://test-ag.ch"


def test_google_search_not_configured(client):
    company_id = _create_company(client)
    with patch(
        "app.api.routes.companies.google_search_client.search_website",
        side_effect=ValueError("GOOGLE_API_KEY and GOOGLE_CSE_ID must be set"),
    ):
        resp = client.get(f"/api/v1/companies/{company_id}/google-search")
    assert resp.status_code == 503
