#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
This module defines an Airflow Hook that sets up a RabbitMQ connection
using the Pika library.
"""
import pika

from airflow.hooks.base_hook import BaseHook


# pylint: disable=abstract-method,too-few-public-methods
# TODO: Update docstring once I add an Airflow connection type.
class RabbitMQHook(BaseHook):
    """
    Hook for RabbitMQ connections.
    This expects a Connection defined in the UI. The connection type is
    ignored since Airflow doesn't have an AMQP type defined.

    Airflow Connection Fields -> RabbitMQ fields
    Required:
    login    -> RabbitMQ username
    password -> RabbitMQ password
    schema   -> RabbitMQ virtual host

    Optional:
    host     -> RabbitMQ server hostname
    port     -> RabbitMQ port
    extras   -> If this is specified, these settings will be passed to
                the pika.ConnectionParameters constructor
    """
    def __init__(self, conn_id):
        # The super constructor doesn't actually do anything
        super(RabbitMQHook, self).__init__(None)
        self.conn_id = conn_id
        self.client = None
        self.channel = None

    def __enter__(self):
        """
        Use RabbitMQHook as a context manager to get a channel that gets
        closed properly

        This is designed for the typical case where you have 1 connection
        and 1 channel. use get_conn() if you need multiple channels

        example:
        with RabbitMQHook(conn_id='rabbitmq') as channel:
            # do something with the RabbitMQChannel
        """
        # Ensure the connection gets created
        conn = self.get_conn()

        # create a channel.
        self.channel = conn.channel()
        return self.channel

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close the channel and the connection gracefully.
        """
        self.channel.close()
        self.channel = None
        self.client.close()
        self.client = None

    def get_conn(self):
        """
        Connect to RabbitMQ. Note that the connection is kept alive until
        closed explicitly. Further calls to get_conn returns the same
        connection.
        """
        if not self.client:
            self._connect_rabbitmq()
        return self.client

    def _connect_rabbitmq(self):
        airflow_conn = self.get_connection(self.conn_id)
        creds = pika.PlainCredentials(
            username=airflow_conn.login,
            password=airflow_conn.password)
        params = pika.ConnectionParameters(
            host=airflow_conn.host,
            port=airflow_conn.port,
            virtual_host=airflow_conn.schema,
            credentials=creds,
            **airflow_conn.extra_dejson)
        self.client = pika.BlockingConnection(params)
