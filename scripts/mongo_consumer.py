import json
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Iniciando Consumidor MongoDB para o Data Lake NoSQL")
    
    consumer = KafkaConsumer(
        'vendas-raw',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='mongo-lake-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    client = MongoClient("mongodb://mongo:27017/")
    db = client["ecommerce"]
    collection = db["transacoes"]

    logger.info("Aguardando mensagens no tópico 'vendas-raw'...")

    try:
        for message in consumer:
            transaction = message.value
            result = collection.insert_one(transaction)
            logger.info(f"Transação salva no Mongo Lake: {transaction['id_transacao']} (MongoID: {result.inserted_id})")
    except KeyboardInterrupt:
        logger.info("\nConsumidor MongoDB interrompido.")
    finally:
        consumer.close()
        client.close()

if __name__ == "__main__":
    main()
