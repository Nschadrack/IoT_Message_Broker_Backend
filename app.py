from flask import Flask, request, Response, json
from utils import MessageBroker, validate_payload, \
    validate_password, validate_username, validate_host, \
    validate_publishers_num, validate_subscribers_num, \
    validate_message_delay_interval, validate_topic_level, \
    validate_subscriber_topic_levels_num, validate_publishers_topic_levels_num, \
    validate_data

app = Flask(__name__)


@app.post('/')
def test_message_broker():
    data = {"username": request.form.get("username", None),
            "password": request.form.get("password", None),
            "host": request.form.get("host", None),
            "port": request.form.get("port", None),
            "topic_level": request.form.get("topic_level", None),
            "publishers_topic_levels_num": request.form.get("publishers_topic_levels_num", None),
            "subscriber_topic_levels_num": request.form.get("subscribers_topic_levels_num", None),
            "payload": request.form.get("payload", None),
            "publishers_num": request.form.get("publishers_num", None),
            "subscribers_num": request.form.get("subscribers_num", None),
            "message_delay_interval": request.form.get("message_delay_interval", None)}

    validation_status, data = validate_data(data)
    if not validation_status:
        return Response(json.dumps({"status": "error",
                                    "data": data}))

    print("\n\nProcessing...\n\n")
    message_broker = MessageBroker(
        username=data["username"],
        password=data["password"],
        host=data["host"],
        port=data["port"],
        topic_level=data["topic_level"],
        publishers_topic_levels_num=data["publishers_topic_levels_num"],
        subscriber_topic_levels_num=data["subscriber_topic_levels_num"],
        payload=data["payload"],
        publishers_num=data["publishers_num"],
        subscribers_num=data["subscribers_num"],
        message_delay_interval=data["message_delay_interval"]
    )

    response_data = message_broker.run()
    if response_data.get("connected"):
        return Response(json.dumps({
            "status": "success",
            "data": response_data}))

    return Response(json.dumps({
        "status": "fail",
        "data": response_data
    }))


if __name__ == '__main__':
    app.run(port=5599)
