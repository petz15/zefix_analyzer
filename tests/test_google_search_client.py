"""Tests for the Google Custom Search client (no real network calls)."""

from unittest.mock import MagicMock, patch

import pytest

from app.api.google_search_client import search_website


class TestSearchWebsite:
    def test_raises_when_no_api_key(self, monkeypatch):
        monkeypatch.setattr("app.api.google_search_client.settings.google_api_key", "")
        monkeypatch.setattr("app.api.google_search_client.settings.google_cse_id", "some-cse")
        with pytest.raises(ValueError, match="GOOGLE_API_KEY"):
            search_website("Test AG")

    def test_raises_when_no_cse_id(self, monkeypatch):
        monkeypatch.setattr("app.api.google_search_client.settings.google_api_key", "key123")
        monkeypatch.setattr("app.api.google_search_client.settings.google_cse_id", "")
        with pytest.raises(ValueError, match="GOOGLE_CSE_ID"):
            search_website("Test AG")

    def test_returns_results(self, monkeypatch):
        monkeypatch.setattr("app.api.google_search_client.settings.google_api_key", "key123")
        monkeypatch.setattr("app.api.google_search_client.settings.google_cse_id", "cse123")

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "items": [
                {"title": "Test AG – Official", "link": "https://test-ag.ch", "snippet": "We are Test AG"},
                {"title": "Test AG on LinkedIn", "link": "https://linkedin.com/company/test-ag"},
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch("app.api.google_search_client.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            results = search_website("Test AG")

        assert len(results) == 2
        assert results[0].link == "https://test-ag.ch"
        assert results[0].snippet == "We are Test AG"
        assert results[1].snippet is None

    def test_empty_response(self, monkeypatch):
        monkeypatch.setattr("app.api.google_search_client.settings.google_api_key", "key123")
        monkeypatch.setattr("app.api.google_search_client.settings.google_cse_id", "cse123")

        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = MagicMock()

        with patch("app.api.google_search_client.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            results = search_website("Unknown Corp")

        assert results == []
