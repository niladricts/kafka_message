# This script is used to connect Kafka services in Aiven and get messages of a particular topic

from kafka import KafkaConsumer
import psycopg2

username = "avnadmin"
password = "uvttknhkhik3y9y4"


class Consumer:
    consumer = KafkaConsumer(
        "python",
        auto_offset_reset="earliest",
        bootstrap_servers="kafka-36d2881a-bsensous-2f1a.aivencloud.com:25041",
        client_id="client1",
        group_id="Group1",
        sasl_mechanism="PLAIN",
        sasl_plain_username=username,
        sasl_plain_password=password,
        security_protocol="SASL_SSL",
        ssl_cafile="ca.pem"
    )

    def receive_message(self, consumer):
        """
        This method is used to receive messages created by the producer and insert into a table
        in postgresql
        :parameter consumer object
        """
        message_id = 0
        for _ in range(10):
            messages = consumer.poll(timeout_ms=1000)
            # print(messages)
            for topic, message in messages.items():
                for msg in message:
                    message_id = message_id + 1
                    print("Received: {}".format(msg.value))
                    try:
                        db_connection = psycopg2.connect(user="admin", password="admin#123",
                                                               host="127.0.0.1", port="5432",
                                                                       database="postgredb")
                        cursor = db_connection.cursor()
                        query = """INSERT INTO MESSAGE(ID,MESSAGE) VALUES(%s,%s)"""
                        values = (message_id, msg)
                        cursor.execute(query, values)
                        count = cursor.rowcount
                        print("Successfully inserted {} records".format(count))
                        db_connection.commit()
                    except (Exception, psycopg2.Error) as Error:
                        print("Insertion failed  due to {}".format(Error))
                    finally:
                             cursor.close()
                             db_connection.close()

        consumer.commit()  # to not get duplicate messages
