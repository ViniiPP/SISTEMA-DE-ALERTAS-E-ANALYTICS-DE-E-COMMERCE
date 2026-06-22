import json
import logging
import redis
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FraudDetectionProcessor(MapFunction):
    def map(self, value):
        try:
            transaction = json.loads(value)
            # regra -valor total > 500
            if transaction.get('valor_total', 0) > 500:
                logger.info(f"ALERTA FRAUDE DETECTADO: {transaction['id_transacao']} - Valor: {transaction['valor_total']}")
                return json.dumps(transaction)
            return None
        except Exception as e:
            logger.error(f"Erro ao processar transacao: {str(e)}")
            return None

class RedisValkeySinkMapper(MapFunction):
    def open(self, runtime_context):
        # conecta noo redis
        self.r = redis.Redis(host='valkey', port=6379, db=0)
        logger.info("Conexao com o Valkey (Redis) estabelecida.")

    def map(self, value):
        transaction = json.loads(value)
        id_transacao = transaction.get('id_transacao')
        
        # salva como chave-valor com prefixo fraude
        self.r.set(f"fraude:{id_transacao}", json.dumps(transaction))
        logger.info(f"Fraude salva no Valkey com sucesso: fraude:{id_transacao}")
        return value

    def close(self):
        if hasattr(self, 'r'):
            self.r.close()

def create_fraud_alert_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_consumer = FlinkKafkaConsumer(
        topics="vendas-raw",
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'flink-fraud-group',
            'auto.offset.reset': 'latest'
        }
    )

    stream = env.add_source(kafka_consumer)
    
    # filtra fraudes
    frauds_stream = stream.map(FraudDetectionProcessor()).filter(lambda x: x is not None)
    
    # salva fraudes no redis
    frauds_stream.map(RedisValkeySinkMapper())

    return env

if __name__ == '__main__':
    create_fraud_alert_job().execute("Job Flink - Alertas de Fraude")
