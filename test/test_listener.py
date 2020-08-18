import mock
import os
import pytest
import sys

from mothrpy.listener import Listener

class TestListener:
    @mock.patch('redis.StrictRedis')
    def setup_method(self, _, mock_redis):
        mock_redis.return_value.pubsub.return_value.listen.return_value = [
            {'pattern': None, 'type': 'message', 'channel': 'test', 'data': 'test message'},        
            {'pattern': 'test:*', 'type': 'pmessage', 'channel': 'test:testing', 'data': 'test message'},        
            {'pattern': None, 'type': 'message', 'channel': 'test', 'data': 'test message2'},        
            {'pattern': None, 'type': 'subscribe', 'channel': 'test', 'data': 'subscribe message'},        
            {'pattern': None, 'type': 'message', 'channel': 'test', 'data': 'test message3'}        
        ]
        self.listener = Listener(['test', 'test:*'])
        self.listener.handle_message = mock.MagicMock()

    def test_run(self):
        self.listener.start()
        assert(self.listener.handle_message.call_count == 4)
