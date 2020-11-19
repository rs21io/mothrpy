# Copyright 2020 Resilient Solutions Inc. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

from __future__ import annotations
import os
import re
import time
from urllib.parse import urlsplit, urlunsplit
from typing import Dict, Iterator, List, Optional, Tuple
from warnings import warn

from gql import gql, Client
from gql.dsl import DSLSchema
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.websockets import WebsocketsTransport


with open(
    os.path.join(os.path.realpath(os.path.dirname(__file__)), "schema.graphql")
) as f:
    schema = f.read()


USERNAME_VAR = "MOTHR_USERNAME"
PASSWORD_VAR = "MOTHR_PASSWORD"
URL_VAR = "MOTHR_ENDPOINT"
TOKEN_VAR = "MOTHR_ACCESS_TOKEN"


class MothrClient:
    """Client for connecting to MOTHR

    Args:
        url (str, optional): Endpoint to send the job request,
            checks for ``MOTHR_ENDPOINT`` in environment variables otherwise
            defaults to ``http://localhost:8080/query``
        token (str, optional): Access token to use for authentication, the library
            also looks for ``MOTHR_ACCESS_TOKEN`` in the environment as a fallback
        username (str, optional): Username for logging in, if not given the library
            will attempt to use ``MOTHR_USERNAME`` environment variable. If neither
            are found the request will be made without authentication.
        password (str, optional): Password for logging in, if not given the library
            will attempt to use the ``MOTHR_PASSWORD`` environment variable. If
            neither are found the request will be made without authentication.
    """

    def __init__(self, **kwargs):
        schemes = {"http": "ws", "https": "wss"}
        self.headers: Dict[str, str] = {}
        endpoint = os.getenv(URL_VAR, "http://localhost:8080/query")
        url = kwargs.pop("url", endpoint)
        split_url = urlsplit(url)
        ws_url = urlunsplit(split_url._replace(scheme=schemes[split_url.scheme]))

        transport = RequestsHTTPTransport(url=url, headers=self.headers)
        client = Client(transport=transport, schema=schema)
        self.ds = DSLSchema(client)

        ws_transport = WebsocketsTransport(url=ws_url)
        self.ws_client = Client(transport=ws_transport, schema=schema)

        self.token = kwargs.pop("token", os.getenv(TOKEN_VAR))
        username = kwargs.pop("username", os.getenv(USERNAME_VAR))
        password = kwargs.pop("password", os.getenv(PASSWORD_VAR))
        if self.token is not None:
            self.headers = {"Authorization": f"Bearer {self.token}"}
        elif all((username, password)):
            self.token, self.refresh = self.login(username, password)
            self.headers = {"Authorization": f"Bearer {self.token}"}

    def login(
        self, username: Optional[str] = None, password: Optional[str] = None
    ) -> Tuple[str, str]:
        """Retrieve a web token from MOTHR

        Args:
            username (str, optional): Username used to login, the library will look
                for ``MOTHR_USERNAME`` in the environment as a fallback.
            password (str, optional): Password used to login, the library will look
                for ``MOTHR_PASSWORD`` in the environment as a fallback.

        Returns:
            str: An access token to pass with future requests
            str: A refresh token for receiving a new access token
                after the current token expires

        Raises:
            ValueError: If a username or password are not provided and are not found
                in the current environment
        """
        username = username if username is not None else os.getenv("MOTHR_USERNAME")
        password = password if password is not None else os.getenv("MOTHR_PASSWORD")
        if username is None:
            raise ValueError("Username not provided")
        if password is None:
            raise ValueError("Password not provided")

        credentials = {"username": username, "password": password}
        q = self.ds.Mutation.login.args(**credentials).select(
            self.ds.LoginResponse.token, self.ds.LoginResponse.refresh
        )
        resp = self.ds.mutate(q)
        tokens = resp["login"]
        if tokens is None:
            raise ValueError("Login failed")
        access = tokens["token"]
        refresh = tokens["refresh"]
        return access, refresh

    def refresh_token(self) -> str:
        """Refresh an expired access token

        Returns:
            str: New access token
        """
        q = self.ds.Mutation.refresh.args(token=self.refresh).select(
            self.ds.RefreshResponse.refresh
        )
        resp = self.ds.mutate(q)
        if resp["refresh"] is None:
            raise ValueError("Token refresh failed")
        token = resp["refresh"]["token"]
        self.token = token
        self.headers["Authorization"] = f"Bearer {self.token}"
        return token


class JobRequest:
    """Class used for managing job requests sent to MOTHR

    Attributes:
        job_id (str): Request job id returned from self.submit()
        status (str): Status of the job

    Args:
        client (MothrClient): Client connection to MOTHR
        service (str): Service being invoked by the request
        parameters (list<dict>, optional): Parameters to pass to the service
        broadcast (list<str>, optional): PubSub channel to broadcast the job result to
        inputs (list<str>, optional): A list of S3 URIs to be used as
            inputs by the service
        outputs (list<str>, optional): A list of S3 URIs to be uploaded by the service
        input_stream (str, optional): Value to pass to service through stdin
        output_metadata (dict): Metadata attached to job outputs
        version (str, optional): Version of the service, default `latest`
    """

    def __init__(self, **kwargs):
        self.client = kwargs.pop("client", MothrClient())
        kwargs["parameters"] = kwargs.get("parameters", [])
        kwargs["outputMetadata"] = kwargs.get("output_metadata", {})
        self.req_args = kwargs
        self.job_id = None
        self.status = None

    @staticmethod
    def is_s3_uri(uri: str) -> bool:
        """Checks if string matches the pattern s3://<bucket>/<key>"""
        return bool(re.match(r"^s3\:\/\/[a-zA-Z0-9\-\.]+[a-zA-Z]\/\S*?$", uri))

    def add_parameter(
        self, value: str, param_type: str = "parameter", name: Optional[str] = None
    ) -> JobRequest:
        """Add an parameter to the job request

        Args:
            value (str): Parameter value
            param_type (str, optional): Parameter type, one of
                (`parameter`, `input`, `output`). Default `parameter`
            name (str, optional): Parameter name/flag (e.g., `-i`, `--input`)
        """
        if self.job_id is not None:
            warn(
                "job has already been submitted, "
                "adding additional parameters will have no effect"
            )
        if param_type in ["input", "output"] and not self.is_s3_uri(value):
            warn(f"{param_type} parameter {value} is not an S3 URI")
        parameter = {"type": param_type, "value": value}
        if name is not None:
            parameter["name"] = name
        self.req_args["parameters"].append(parameter)
        return self

    def add_input(self, value: str, name: Optional[str] = None) -> JobRequest:
        """Add an input parameter to the job request"""
        return self.add_parameter(value, param_type="input", name=name)

    def add_output(self, value: str, name: Optional[str] = None) -> JobRequest:
        """Add an output parameter to the job request"""
        return self.add_parameter(value, param_type="output", name=name)

    def add_output_metadata(self, metadata: Dict[str, str]) -> JobRequest:
        """Add metadata to job outputs

        Args:
            metadata (dict)
        """
        if self.job_id is not None:
            warn(
                "job has already been submitted, "
                "adding additional output metadata will have no effect"
            )
        self.req_args["outputMetadata"].update(metadata)
        return self

    def submit(self) -> str:
        """Submit the job request

        Returns:
            str: The unique job identifier
        """
        metadata = [{"key": k, "value": v} for k, v in self.req_args["outputMetadata"]]
        self.req_args["outputMetadata"] = metadata
        q = self.client.ds.Mutation.submit_job.args(request=self.req_args).select(
            self.client.ds.JobRequestResponse.job.select(
                self.client.ds.Job.job_id, self.client.ds.Job.status
            )
        )
        resp = self.client.ds.mutate(q)
        if "errors" in resp:
            raise ValueError("Error submitting job request: " + resp["errors"])
        job_id = resp["submitJob"]["job"]["jobId"]
        status = resp["submitJob"]["job"]["status"]
        self.job_id = job_id
        self.status = status
        return job_id

    def query_job(self, fields: List[str]) -> Dict[str, str]:
        """Query information about the job request

        Args:
            fields (list<str>): Fields to return in the query response

        Returns:
            dict: Query result for the job request

        Raises:
            ValueError: If job ID does not exist
        """
        if self.job_id is None:
            raise ValueError("Job ID is None, have you submitted the job?")
        fields = [getattr(self.client.ds.Job, field) for field in fields]
        q = self.client.ds.Query.job.args(jobId=self.job_id).select(*fields)
        resp = self.client.ds.query(q)
        return resp["job"]

    def check_status(self) -> str:
        """Check the current status of the job request

        Returns:
            str: Job status
        """
        job = self.query_job(fields=["status"])
        return job["status"]

    def result(self) -> Dict[str, str]:
        """Get the job result

        Returns:
            dict: Complete response from the job query
        """
        job = self.query_job(fields=["jobId", "service", "status", "result", "error"])
        return job

    def subscribe(self) -> str:
        """Subscribe to the job's complete event"""
        s = gql(
            f"""
            subscription {{
                subscribeJobComplete(jobId: "{self.job_id}") {{
                    jobId
                    service
                    status
                    result
                    error
                }}
            }}
        """
        )
        result = list(self.client.ws_client.subscribe(s))
        return result[0]

    def subscribe_messages(self) -> Iterator[str]:
        """Subscribe to intermediate messages published by the job"""
        s = gql(
            f"""
            subscription {{
                subscribeJobMessages(jobId: "{self.job_id}")
            }}
        """
        )
        for result in self.client.ws_client.subscribe(s):
            yield result

    def run_job(
        self, poll_frequency: float = 0.25, return_failed: bool = False
    ) -> Dict[str, str]:
        """Execute the job request

        Args:
            poll_frequency (float, optional): Frequency, in seconds, to poll for job
                status. Default, poll 0.25 seconds.
            return_failed (bool, optional): Return failed job results instead of
                raising an exception. Default False

        Returns:
            dict: The job result

            Example::

                {
                    'jobId': 'job-id',
                    'service': 'my-service',
                    'status': 'complete',
                    'result': '',
                    'error': ''
                }

        Raises:
            RuntimeError: If job returns a status of failed, unless explicitly
                specified to return failed jobs by setting `return_failed`
                parameter to True
        """
        job_id = self.submit()
        status = self.check_status()
        while status in ["submitted", "running"]:
            status = self.check_status()
            time.sleep(poll_frequency)
        result = self.result()
        if status != "complete" and return_failed is False:
            raise RuntimeError("Job {} failed: {}".format(job_id, result["error"]))
        return result
