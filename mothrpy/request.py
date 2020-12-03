# Copyright 2020 Resilient Solutions Inc. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

from __future__ import annotations
import re
import time
from typing import Dict, Iterator, List, Optional
from warnings import warn

from gql import gql
from .client import MothrClient


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
        fields = [
            self.client.resolve_field(self.client.ds.Job, field) for field in fields
        ]
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
