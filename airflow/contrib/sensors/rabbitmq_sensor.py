"""
This module defines a RabbitMQSensor that continuously polls a RabbitMQ queue
periodically to wait for a message that indicates whether the task succeeded.
"""
import logging
import contextlib

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.contrib.hooks.rabbitmq_hook import RabbitMQHook


class RabbitMQSensor(BaseSensorOperator):
    """
    Poll a RabbitMQ queue

    This assumes a topic exchange since it is a very flexible message routing
    scheme.
    """

    ui_color = "#280026"
    ui_fgcolor = "#ffffff"

    # Airflow sensor return values.
    SUCCESS = True
    WAIT = False
    # The callback will return this exception object and the execute()
    # method will raise it after cleaning up
    ERROR = AirflowException

    # Airflow operators can only pass arguments in the constructor so there
    # are often many of them
    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
            self,
            conn_id,
            exchange_name,
            topics,
            message_callback,
            queue_name=None,
            **kwargs):
        """
        Constructor

        :param str conn_id: ID of the Airflow Connection object
        :param str exchange_name: the name of the topic exchange to listen
            to.
        :param list(str) topics: topics to listen to. It can include the
            wildcards * and #. See the RabbitMQ docs for more information.
        :param func(channel, method, header, body) message_callback:
            callback function called for every message received. The function
            should return one of three things:
            1. RabbitMQSensor.SUCCESS - The message indicated success.
            2. RabbitMQSensor.ERROR("message") - This message indicates
                an error. Provide a message explaining the error.
            3. RabbitMQSensor.WAIT - The message is irrelevant to this sensor,
                wait for the next one.
        :param str queue_name: If specified, this queue will be used.
            Otherwise, a temporary queue will be used.
        """
        super(RabbitMQSensor, self).__init__(**kwargs)
        self.hook = RabbitMQHook(conn_id=conn_id)
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.topics = topics
        self.message_callback = message_callback

    def declare_exchange(self, channel):
        """
        Declare an exchange on the RabbitMQ Server
        """
        channel.exchange_declare(
            exchange=self.exchange_name, exchange_type='topic', durable=True)

    def declare_queue(self, channel):
        """
        Declare a queue on the RabbitMQ. If no queue_name was specified,
        declare a temporary queue
        """
        # If the queue is not specified, make a temp queue
        if not self.queue_name:
            queue = channel.queue_declare(exclusive=True, durable=True)
            self.queue_name = queue.method.queue
        else:
            channel.queue_declare(self.queue_name, durable=True)

    def bind_topics(self, channel):
        """
        Bind the queue to the exchange with the topics specified in the
        constructor
        """
        for topic in self.topics:
            channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
                routing_key=topic)

    def poll_queue(self, channel, task_instance):
        """
        Poll the queue once to check for a status metssage
        """
        # Grab one message from the queue
        method_frame, header_frame, body = channel.basic_get(self.queue_name)

        # If we didn't get a message, wait for the next poll interval
        if method_frame is None:
            return False

        # Pass the message downstream regardless of success/failure
        # if the message is ignored by the user callback, the value will
        # be overwritten with the next message
        task_instance.xcom_push('payload', body)

        # The user needs to define whether the message classifies as success,
        # failure, or should be ignored.
        result = self.message_callback(
            channel, method_frame, header_frame, body)

        # Sanity check: Did we return True, False or an AirflowException?
        if not isinstance(result, (bool, AirflowException)):
            raise ValueError(
                "Callback returned {}. Return value must be "
                "one of RabbitMQSensor.SUCCESS, "
                "RabbitMQSensor.WAIT, or"
                "RabbitMQSensor.ERROR(message)".format(result))

        # Let's acknowledge that we recieved the  message
        channel.basic_ack(method_frame.delivery_tag)

        # if result is an exception, raise it, else return it.
        if isinstance(result, self.ERROR):
            raise result

        return result

    def execute(self, context):
        """
        Operate like a normal sensor, but start the connection before the
        loop and close it at the end gracefully.
        """
        with self.hook.get_conn():
            super(RabbitMQSensor, self).execute(context)

    def poke(self, context):
        """
        This function is run over and over until we get a message from
        RabbitMQ or we time out.
        """
        logging.info("Checking RabbitMQ...")
        logging.info("Context %s", context)

        # get_conn() only connects the first time, so this just fetches
        # an existing connection
        conn = self.hook.get_conn()

        # Create a channel every time we go through this loop
        channel = conn.channel()
        with contextlib.closing(channel):
            self.declare_exchange(channel)
            self.declare_queue(channel)
            self.bind_topics(channel)
            return self.poll_queue(channel, context['task_instance'])
