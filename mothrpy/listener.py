# Copyright 2020 Resilient Solutions Inc. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

import gc
import os
import redis
from argparse import ArgumentParser
from typing import Dict, Iterator, List, Optional, Union

try:
    import gevent
    USE_THREADING = True
except ImportError:
    USE_THREADING = False

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')


class Listener:
    """Object that listens and responds to specific events triggered on the system.

    This class is intended to be used as a base class for creating listeners. To use
    simply inherit and override the ``handle_message`` method.

    Args:
        channels (str, :obj:`list` of :obj:`str`): Event channel(s) to listen on.
            These can be explicit names (e.g., channel:1) or patterns with a
            wildcard (e.g., channel:*) which will listen to all channels that start
            with "channel:".
    """
    def __init__(self, channels: List[str]) -> None:
        db = redis.StrictRedis(REDIS_HOST, decode_responses=True)
        pubsub = db.pubsub()
        if isinstance(channels, str):
            channels = [channels]
        subs = [channel for channel in channels if not channel.endswith('*')]
        pattern_subs = [channel for channel in channels if channel.endswith('*')]
        if len(subs) > 0:
            pubsub.subscribe(*subs)
        if len(pattern_subs) > 0:
            pubsub.psubscribe(*pattern_subs)

        self.pubsub = pubsub

    def _iter_data(self) -> Iterator[Dict[str, Union[None, str]]]:
        """Yield published messages received on subscribed channels"""
        for message in self.pubsub.listen():
            if message['type'] in ['message', 'pmessage']:
                yield message

    def handle_message(self, channel, message):
        """Handler for received messages

        Args:
            channel (str): Name of the channel on which the message was received
            message (str): The message received on the channel
        """
        raise NotImplementedError('You must override handle_message to use this class')

    def run(self) -> None:
        """Listen for messages on subscribed channels"""
        for message in self._iter_data():
            if USE_THREADING:
                gevent.spawn(self.handle_message, message['channel'], message['data'])
                gevent.sleep(0)
            else:
                self.handle_message(message['channel'], message['data'])

    def start(self) -> None:
        print('Starting listener')
        try:
            if not USE_THREADING:
                print('''WARNING: gevent library is not installed, concurrency is disabled. \
                            Listener will block when handling messages and potentially \
                            miss events, enable concurrency by installing optional \
                            dependencies with "pip install mothrpy[listener]"''')
            self.run()
        except KeyboardInterrupt:
            print('Stopping listener')
        except Exception as e:
            print(e)
        finally:
            if USE_THREADING:
                gevent.killall([obj for obj in gc.get_objects()
                                if isinstance(obj, gevent.Greenlet)])
            self.pubsub.unsubscribe()
            self.pubsub.punsubscribe()
