# The main script to run the producer consumer communication
from producer import Producer
from consumer import Consumer

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    p = Producer()
    c = Consumer()
    p.produce_message(Producer.producer)
    c.receive_message(Consumer.consumer)

