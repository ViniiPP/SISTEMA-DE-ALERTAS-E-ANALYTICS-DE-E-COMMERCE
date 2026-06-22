import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

def generate_transaction(faker):
    is_fraud = random.random() < 0.1
    valor = round(random.uniform(500.0, 5000.0), 2) if is_fraud else round(random.uniform(10.0, 499.0), 2)
    categorias = ["Eletrônicos", "Roupas", "Casa", "Brinquedos", "Beleza"]
    
    transaction = {
        "id_transacao": str(uuid.uuid4()),
        "id_cliente": faker.random_int(min=1, max=10000),
        "valor_total": valor,
        "categoria": random.choice(categorias),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    return transaction

def main():
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    faker = Faker('pt_BR')
    print("Iniciando envio de transações sintéticas para o tópico 'vendas-raw'...")
    
    try:
        while True:
            trans = generate_transaction(faker)
            producer.send('vendas-raw', value=trans)
            print(f"Enviado: {trans}")
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print("\nProdutor interrompido pelo usuário.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
