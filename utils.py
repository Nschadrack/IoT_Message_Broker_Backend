import time
import paho.mqtt.client as paho
from paho import mqtt
import uuid
import psutil


class MessageBroker:
    """
        an abstract for actual message broker
    """

    def __init__(self, username: str, password: str, host: str, topic_level: str,
                 publishers_topic_levels_num: int, subscriber_topic_levels_num: int,
                 payload: str, publishers_num: int, subscribers_num: int,
                 message_delay_interval, userdata=None, protocol=paho.MQTTv5, port=8883):

        self.username = username
        self.password = password
        self.host = host
        self.topic_level = topic_level
        self.publishers_topic_levels_num = publishers_topic_levels_num
        self.subscriber_topic_levels_num = subscriber_topic_levels_num
        self.payload = payload
        self.publishers_num = publishers_num
        self.subscribers_num = subscribers_num
        self.subscribers = []
        self.publishers = []
        self.userdata = userdata
        self.protocol = protocol
        self.port = port
        self.message_delay_interval = message_delay_interval
        self.client_connected = False

        self.dashboard_statistics = {
            "sent_messages": 0,
            "received_messages": 0,
            "failed_messages": 0,
            "cpu_used": 0,
            "subscribers": 0,
            "publishers": 0,
            "memory_used": 0,
            "network_in": 0,
            "network_out": 0
        }

    def on_connect(self, client, userdata, flags, rc, properties=None):
        """
        Helps to check if client was connected successful
        """
        if str(rc).lower() == "success":
            self.client_connected = True
        else:
            self.client_connected = False

    def on_message(self, client, userdata, msg):
        """
        Helps to track if message was successfully delivered to subscriber
        """
        self.dashboard_statistics["received_messages"] += 1

    def connect_client_to_broker(self):
        client = paho.Client(client_id=str(uuid.uuid4()),
                             userdata=self.userdata,
                             protocol=self.protocol)

        client.on_connect = self.on_connect  # mapping on_connect callback
        client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)  # Enabling TLS for secure connection
        client.username_pw_set(self.username, self.password)  # set username and password
        client.connect(host=self.host, port=self.port)  # connecting to HiveMQ

        client.on_message = self.on_message
        client.loop_start()
        return client

    def subscribe_client_to_topic(self, client, j, qos=1):
        client.subscribe(f"{self.topic_level.rstrip('/') + '/' + str(j) + '/'}", qos=qos)

    def publish_to_broker(self, client, j, qos=1):
        status, _ = client.publish(f"{self.topic_level.rstrip('/') + '/' + str(j) + '/'}", payload=self.payload,
                                   qos=qos)

        if status == 0:
            self.dashboard_statistics["sent_messages"] += 1
        else:
            self.dashboard_statistics["failed_messages"] += 1

    def run(self):
        """
        method for running broker
        :return:
        """
        # run subscribers and publishers
        # do calculations
        # return statistics or error

        starting_time = time.time()
        #  generating subscribers
        i = 0
        while i <= self.subscribers_num:
            client = self.connect_client_to_broker()
            if self.client_connected:
                self.dashboard_statistics["subscribers"] += 1
                #  subscribing to different topic levels
                for j in range(self.subscriber_topic_levels_num):
                    self.subscribe_client_to_topic(client, j)
                self.subscribers.append(client)
            i += 1

        #  Generating publishers
        i -= i  # taking it back to zero
        publishing_start_time = time.time()  # Tracking the time it takes to send and receive messages
        while i < self.publishers_num:
            client = self.connect_client_to_broker()
            if self.client_connected:
                self.dashboard_statistics["publishers"] += 1

                #  publishing message to different topic levels
                for j in range(self.publishers_topic_levels_num):
                    self.publish_to_broker(client, j)
                    time.sleep(self.message_delay_interval)  # delay
                self.publishers.append(client)
            i += 1

        publishing_end_time = time.time()

        publishing_total_time = publishing_end_time - publishing_start_time
        received_messages_in_sec = int(self.dashboard_statistics["received_messages"] / publishing_total_time)
        sent_messages_in_sec = int(self.dashboard_statistics["sent_messages"] / publishing_total_time)

        self.dashboard_statistics["received_messages_in_sec"] = received_messages_in_sec
        self.dashboard_statistics["sent_messages_in_sec"] = sent_messages_in_sec
        self.dashboard_statistics["memory_used"] = psutil.virtual_memory().used // (1024 * 1024)
        self.dashboard_statistics["cpu_used"] = psutil.cpu_percent(interval=int(time.time() - starting_time))

        # use psutil for network in and out
        bytes_sent = self.dashboard_statistics["sent_messages_in_sec"] * len(self.payload) * 8  # converting to bits
        bytes_received = self.dashboard_statistics["received_messages_in_sec"] * len(
            self.payload) * 8  # converting to bits
        self.dashboard_statistics["network_in"] = int(bytes_received / 1000)
        self.dashboard_statistics["network_out"] = int(bytes_sent / 1000)

        #  disconnecting subscribers and stopping them
        for subscriber in self.subscribers:
            subscriber.loop_stop()
            subscriber.disconnect()
        self.subscribers.clear()

        #  disconnecting publishers and stopping them
        for publisher in self.publishers:
            publisher.loop_stop()
            publisher.disconnect()
        self.publishers.clear()

        if self.dashboard_statistics["subscribers"] == 0 and self.dashboard_statistics["publishers"] == 0:
            return {"connected": False, "dashboard_statistics": {}}

        return {"connected": True, "dashboard_statistics": self.dashboard_statistics}


def validate_username(username):
    return True if username else False


def validate_password(password):
    return True if password else False


def validate_host(host):
    return True if host else False


def validate_topic_level(topic_level):
    if topic_level:
        topic_level = topic_level.rstrip("#")
        return topic_level
    return False


def validate_publishers_topic_levels_num(publishers_topic_levels_num):
    try:
        publishers_topic_levels_num = int(float(publishers_topic_levels_num))
        return publishers_topic_levels_num
    except ValueError:
        return False


def validate_subscriber_topic_levels_num(subscriber_topic_levels_num):
    try:
        subscriber_topic_levels_num = int(float(subscriber_topic_levels_num))
        return subscriber_topic_levels_num
    except ValueError:
        return False


def validate_payload(payload):
    return True if payload else False


def validate_publishers_num(publishers_num):
    try:
        publishers_num = int(float(publishers_num))
        return publishers_num
    except ValueError:
        return False


def validate_subscribers_num(subscribers_num):
    try:
        subscribers_num = int(float(subscribers_num))
        return subscribers_num
    except ValueError:
        return False


def validate_message_delay_interval(message_delay_interval):
    try:
        message_delay_interval = float(message_delay_interval)
        return message_delay_interval
    except ValueError:
        return False


def validate_port(port):
    try:
        port = int(port)
        return port
    except ValueError:
        return False


def validate_data(data):
    errors = {}
    if not validate_username(data["username"]):
        errors["username"] = "Username is not valid"

    if not validate_password(data["password"]):
        errors["password"] = "Password cannot be empty"

    if not validate_host(data["host"]):
        errors["host"] = "Host cannot be empty"

    if not validate_topic_level(data["topic_level"]):
        errors["topic_level"] = "Topic level is not valid"
    else:
        data["topic_level"] = validate_topic_level(data["topic_level"])

    if not validate_publishers_topic_levels_num(data["publishers_topic_levels_num"]):
        errors["publishers_topic_levels_num"] = "Expected publishers topic levels num to be a number"
    else:
        data["publishers_topic_levels_num"] = validate_publishers_topic_levels_num(data["publishers_topic_levels_num"])

    if not validate_subscriber_topic_levels_num(data["subscriber_topic_levels_num"]):
        errors["subscriber_topic_levels_num"] = "Expected subscriber topic levels num to be a number"
    else:
        data["subscriber_topic_levels_num"] = validate_subscriber_topic_levels_num(data["subscriber_topic_levels_num"])

    if not validate_payload(data["payload"]):
        errors["payload"] = "Message cannot be empty"

    if not validate_publishers_num(data["publishers_num"]):
        errors["publishers_num"] = "Expected publishers num to be a number"
    else:
        data["publishers_num"] = validate_publishers_num(data["publishers_num"])

    if not validate_subscribers_num(data["subscribers_num"]):
        errors["subscribers_num"] = "Expected subscribers num to be a number"
    else:
        data["subscribers_num"] = validate_subscribers_num(data["subscribers_num"])

    if not validate_message_delay_interval(data["message_delay_interval"]):
        errors["message_delay_interval"] = "Expected delay interval to be a number"
    else:
        data["message_delay_interval"] = validate_message_delay_interval(data["message_delay_interval"])

    if not validate_port(data["port"]):
        errors["port"] = "Expected port to be a number"
    else:
        data["port"] = validate_port(data["port"])

    if len(errors.keys()) != 0:
        return False, errors
    return True, data


if __name__ == "__main__":
    broker = MessageBroker(
        username="Nschadrack",
        password="schadrack1@",
        host="6547b78076d24022a6055cda972c86a7.s2.eu.hivemq.cloud",
        topic_level="sports/football/",
        publishers_topic_levels_num=50,
        subscriber_topic_levels_num=100,
        payload="The win of Manchester City was real good for the team and the team deserved it",
        publishers_num=40,
        subscribers_num=50,
        message_delay_interval=.001
    )
    print("\nProcessing...\n")
    print("Statistics: ", broker.run())
