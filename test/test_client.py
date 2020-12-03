import mock
import pytest
from mothrpy import MothrClient


class TestMothrClient:
    @mock.patch("gql.dsl.DSLSchema.mutate")
    def test_login(self, mock_mutate):
        mock_mutate.return_value = {
            "login": {"token": "access-token", "refresh": "refresh-token"}
        }
        client = MothrClient()
        access, refresh = client.login(username="test", password="password")
        assert access == "access-token"
        assert refresh == "refresh-token"

    @mock.patch("gql.dsl.DSLSchema.mutate")
    def test_refresh(self, mock_mutate):
        mock_mutate.side_effect = [
            {"login": {"token": "access-token", "refresh": "refresh-token"}},
            {"refresh": {"token": "refreshed-access-token"}},
        ]
        client = MothrClient()
        client.login(username="test", password="password")
        access = client.refresh_token()
        assert access == "refreshed-access-token"

    @mock.patch("gql.dsl.DSLSchema.query")
    def test_service(self, mock_query):
        mock_query.return_value = {
            "service": [
                {
                    "name": "test",
                    "version": "latest",
                    "parameters": [{"name": "param1", "fileType": {"name": "text"}}],
                },
                {
                    "name": "test",
                    "version": "dev",
                    "parameters": [{"name": "param1", "fileType": {"name": "text"}}],
                },
            ]
        }
        client = MothrClient()
        service = client.service(
            name="test",
            fields=["name", "version", "parameters.name", "parameters.fileType.name"],
        )
        assert len(service) == 2

    @mock.patch("gql.dsl.DSLSchema.query")
    def test_services(self, mock_query):
        mock_query.return_value = {
            "services": [
                {"name": "test-service", "version": "latest"},
                {"name": "test-service", "version": "dev"},
                {"name": "test-service2", "version": "latest"},
                {"name": "test-service2", "version": "dev"},
            ]
        }
        client = MothrClient()
        services = client.services()
        assert len(services) == 4
