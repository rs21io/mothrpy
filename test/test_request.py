import mock
import pytest
from mothrpy import JobRequest

class TestJobRequest:
    def setup_method(self, _):
        self.submit_response = {'submitJob': {'job': {'jobId': 'test', 'status': 'submitted'}}}
        self.query_response = [
            {'job': {'status': 'submitted'}}, 
            {'job': {'status': 'running'}}, 
            {'job': {'status': 'complete'}}, 
            {'job': {'status': 'complete'}}
        ]

    @mock.patch('gql.dsl.DSLSchema.mutate')
    @mock.patch('gql.dsl.DSLSchema.query')
    def test_run_job(self, mock_query, mock_mutate):
        mock_mutate.return_value = self.submit_response
        mock_query.side_effect = self.query_response
        request = JobRequest(service='test')
        result = request.run_job()
        assert(result)
        
    @mock.patch('gql.dsl.DSLSchema.mutate')
    @mock.patch('gql.dsl.DSLSchema.query')
    def test_run_job_fail(self, mock_query, mock_mutate):
        for i in [-1, -2]:
            self.query_response[i]['job']['status'] = 'failed'
            self.query_response[i]['job']['error'] = 'failed'
        mock_mutate.return_value = self.submit_response
        mock_query.side_effect = self.query_response
        request = JobRequest(service='test')
        with pytest.raises(RuntimeError):
            request.run_job()
        
    @mock.patch('gql.dsl.DSLSchema.mutate')
    @mock.patch('gql.dsl.DSLSchema.query')
    def test_run_job_fail_return_failed(self, mock_query, mock_mutate):
        for i in [-1, -2]:
            self.query_response[i]['job']['status'] = 'failed'
        mock_mutate.return_value = self.submit_response
        mock_query.side_effect = self.query_response
        request = JobRequest(service='test')
        result = request.run_job(return_failed=True)
        assert(result['status'] == 'failed')
        
    @mock.patch('gql.Client.subscribe')
    def test_subscribe(self, mock_subscribe):
        mock_subscribe.return_value = ['test']
        request = JobRequest(service='test')
        result = request.subscribe()
        assert(result == 'test')
        
    @mock.patch('gql.Client.subscribe')
    def test_subscribe_messages(self, mock_subscribe):
        mock_subscribe.return_value = [f'message {i+1}' for i in range(10)]
        request = JobRequest(service='test')
        messages = [m for m in request.subscribe_messages()]
        assert(len(messages) == 10)
        assert(messages[0] == 'message 1')
        
    def test_add_argument_chaining(self):
        request = JobRequest(service='test')
        request.add_input(value='foo') \
               .add_output(value='bar') \
               .add_parameter(value='baz') \
               .add_output_metadata({'foo': 'bar'})
        assert(len(request.req_args['parameters']) == 3)
        assert(request.req_args['parameters'][0]['type'] == 'input')
        assert(request.req_args['parameters'][1]['type'] == 'output')
        assert(request.req_args['parameters'][2]['type'] == 'parameter')
        assert(request.req_args['outputMetadata']['foo'] == 'bar')

    @mock.patch('gql.dsl.DSLSchema.mutate')
    def test_login(self, mock_mutate):
        mock_mutate.return_value = {'login':{'token':'access-token','refresh':'refresh-token'}}
        request = JobRequest(service='test')
        access, refresh =  request.login(username='test', password='password')
        assert(access == 'access-token')
        assert(refresh == 'refresh-token')
