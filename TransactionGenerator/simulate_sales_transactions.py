import json
import random
import time
import logging
from datetime import datetime
from faker import Faker
from confluent_kafka import SerializingProducer

fake = Faker()
logger = logging.getLogger(__name__)

def generate_sales_transactions():
    """
    Generate a simulated sales transaction.

    Returns:
        dict: A dictionary representing a sales transaction with keys:
            - transactionId: Unique identifier for the transaction.
            - productId: ID of the product being sold.
            - productName: Name of the product being sold.
            - productCategory: Category of the product.
            - productPrice: Price of the product.
            - productQuantity: Quantity of the product being sold.
            - productBrand: Brand of the product.
            - currency: Currency of the transaction.
            - customerId: ID of the customer making the purchase.
            - transactionDate: Date and time of the transaction.
            - paymentMethod: Payment method used for the transaction.
    """
    user = fake.simple_profile()
    return {
        "transactionId": fake.uuid4(),
        "productId": random.choice(['product1', 'product2', 'product3', 'product4', 'product5', 'product6']),
        "productName": random.choice(['laptop', 'mobile', 'tablet', 'watch', 'headphone', 'speaker']),
        'productCategory': random.choice(['electronic', 'fashion', 'grocery', 'home', 'beauty', 'sports']),
        'productPrice': round(random.uniform(10, 1000), 2),
        'productQuantity': random.randint(1, 10),
        'productBrand': random.choice(['apple', 'samsung', 'oneplus', 'mi', 'boat', 'sony']),
        'currency': random.choice(['USD', 'GBP']),
        'customerId': user['username'],
        'transactionDate': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer'])
    }

def delivery_report(err, msg):
    """
    Callback function to handle message delivery status.

    Args:
        err (KafkaError): Error, if any, that occurred during message delivery.
        msg (Message): The message object containing delivery status.
    """
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f"Message delivered to {msg.topic} [{msg.partition()}]")

def main():
    """
    Main function to generate and publish sales transactions to Kafka.
    """
    topic = 'financial_transactions'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092'
    })

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 120:
        try:
            transaction = generate_sales_transactions()
            transaction['totalAmount'] = transaction['productPrice'] * transaction['productQuantity']

            logger.debug(transaction)

            producer.produce(topic,
                             key=transaction['transactionId'],
                             value=json.dumps(transaction),
                             on_delivery=delivery_report
                             )
            producer.poll(0)

            # wait for 5 seconds before sending the next transaction
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("User interrupted, exiting...")
            break
        except BufferError:
            logger.warning("Buffer full! Waiting...")
            time.sleep(1)
        except Exception as e:
            logger.exception(e)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
