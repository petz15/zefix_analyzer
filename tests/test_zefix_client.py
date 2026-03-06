"""Tests for the Zefix API client helper functions (no network calls)."""

from unittest.mock import MagicMock, patch

import pytest

from app.api.zefix_client import _normalise_uid, _parse_company, search_companies, get_company


class TestNormaliseUid:
    def test_nine_digit_string(self):
        assert _normalise_uid("123456789") == "CHE-123.456.789"

    def test_formatted_uid_unchanged(self):
        # Already formatted – digits only = 9, still normalised
        assert _normalise_uid("CHE-123.456.789") == "CHE-123.456.789"

    def test_non_nine_digit_returns_as_is(self):
        assert _normalise_uid("abc") == "abc"

    def test_uid_with_dashes_and_dots(self):
        # Strip non-digit chars and reformat
        assert _normalise_uid("CHE-456.789.012") == "CHE-456.789.012"


class TestParseCompany:
    def test_basic_fields(self):
        data = {
            "uid": "123456789",
            "name": "Test AG",
            "legalForm": {"de": "Aktiengesellschaft"},
            "status": "ACTIVE",
            "municipality": "Zurich",
            "canton": "ZH",
        }
        result = _parse_company(data)
        assert result.uid == "CHE-123.456.789"
        assert result.name == "Test AG"
        assert result.legal_form == "Aktiengesellschaft"
        assert result.status == "ACTIVE"
        assert result.municipality == "Zurich"
        assert result.canton == "ZH"

    def test_name_as_dict(self):
        data = {
            "uid": "111222333",
            "name": {"de": "Beispiel GmbH", "fr": "Exemple Sàrl"},
        }
        result = _parse_company(data)
        assert result.name == "Beispiel GmbH"

    def test_missing_optional_fields(self):
        data = {"uid": "000000001", "name": "Minimal SA"}
        result = _parse_company(data)
        assert result.legal_form is None
        assert result.status is None
        assert result.municipality is None
        assert result.canton is None


class TestSearchCompanies:
    def test_returns_list_of_results(self):
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"uid": "123456789", "name": "Test AG", "status": "ACTIVE"},
        ]
        mock_response.raise_for_status = MagicMock()

        with patch("app.api.zefix_client.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.post.return_value = mock_response
            mock_client_cls.return_value = mock_client

            results = search_companies("Test")

        assert len(results) == 1
        assert results[0].name == "Test AG"

    def test_handles_nested_list_response(self):
        mock_response = MagicMock()
        mock_response.json.return_value = {"list": [{"uid": "987654321", "name": "Another GmbH"}]}
        mock_response.raise_for_status = MagicMock()

        with patch("app.api.zefix_client.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.post.return_value = mock_response
            mock_client_cls.return_value = mock_client

            results = search_companies("Another")

        assert len(results) == 1
        assert results[0].name == "Another GmbH"
