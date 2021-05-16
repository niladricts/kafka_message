# This script is used to connect Kafka services in Aiven and send messages

from kafka import KafkaProducer

username = "avnadmin"
password = "uvttknhkhik3y9y4"


class Producer:
    producer = KafkaProducer(
        bootstrap_servers="kafka-36d2881a-bsensous-2f1a.aivencloud.com:25041",
        sasl_mechanism="PLAIN",
        sasl_plain_password=password,
        sasl_plain_username=username,
        security_protocol="SASL_SSL",
        ssl_cafile="ca.pem"
    )

    def produce_message(self, producer):
        """
        This method is required to create the messages by connecting Kafka for a specific topic
        :parameter producer object
        """
        for i in range(10):
            message = "message number {}".format(i)
            print("Sending {}".format(message))
            producer.send("python", message.encode("utf-8"))

            # to force sending all messages
            producer.flush(1000)
