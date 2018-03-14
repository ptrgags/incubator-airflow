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
This module defines a RabbitMQOperator. It is similar to a PythonOperator
except the callback accepts a RabbitMQ channel for interacting with a
RabbitMQ server.
"""
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.contrib.hooks.rabbitmq_hook import RabbitMQHook


# pylint: disable=too-few-public-methods
class RabbitMQOperator(BaseOperator):
    """
    Similar to a PythonOperator except the callback is provided a
    Pika Channel object.
    """
    ui_color = "#581d56"
    ui_fgcolor = "#ffffff"

    @apply_defaults
    def __init__(self, conn_id, rabbit_callback, **kwargs):
        """
        Consructor

        :param str conn_id: the ID of the RabbitMQ Connection in Airflow
        :param func(channel, **context) rabbit_callback: This function will
            always be passed the context.
        """
        super(RabbitMQOperator, self).__init__(**kwargs)
        self.callback = rabbit_callback
        self.hook = RabbitMQHook(conn_id=conn_id)

    def execute(self, context):
        """
        Connect to RabbitMQ and run the callback
        """
        # Run the callback. The connection will always get closed gracefully
        with self.hook as channel:
            return self.callback(channel, **context)
