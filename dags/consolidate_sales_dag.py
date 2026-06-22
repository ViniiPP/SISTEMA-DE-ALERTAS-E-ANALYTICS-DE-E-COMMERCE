from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def consolidate_data():
    print("Iniciando consolidacao de vendas...")
    
    # conexão com mongodb
    mongo_client = MongoClient("mongodb://mongo:27017/")
    db = mongo_client["ecommerce"]
    collection = db["transacoes"]

    # pipeline de agregação: agrupa por categoria e soma o valor_total
    pipeline = [
        {
            "$group": {
                "_id": "$categoria",
                "total_faturado": {"$sum": "$valor_total"}
            }
        }
    ]
    
    resultados = list(collection.aggregate(pipeline))
    print(f"Foram encontradas {len(resultados)} categorias no Mongo.")

    if not resultados:
        print("Nenhuma transação encontrada no Mongo.")
        return

    # Conexão com Postgres  credenciais definidas no docker-compose
    conn = psycopg2.connect(
        dbname="bigdata",
        user="admin",
        password="admin",
        host="postgres",
        port="5432"
    )
    cursor = conn.cursor()

    for row in resultados:
        categoria = row["_id"]
        total = row["total_faturado"]
        
        print(f"Atualizando Postgres: Categoria={categoria}, Total={total}")
        
        query = """
            INSERT INTO faturamento_categoria (categoria, faturamento_total, ultima_atualizacao)
            VALUES (%s, %s, %s)
            ON CONFLICT (categoria) 
            DO UPDATE SET 
                faturamento_total = EXCLUDED.faturamento_total,
                ultima_atualizacao = EXCLUDED.ultima_atualizacao;
        """
        cursor.execute(query, (categoria, total, datetime.now()))
    
    conn.commit()
    cursor.close()
    conn.close()
    mongo_client.close()
    print("Consolidacao finalizada com sucesso.")

with DAG(
    'consolidate_sales_dag',
    default_args=default_args,
    description='Extrai dados do Mongo, consolida faturamento e salva no Postgres',
    schedule_interval=timedelta(minutes=2),
    catchup=False
) as dag:

    consolidar_task = PythonOperator(
        task_id='consolidar_vendas_postgres',
        python_callable=consolidate_data
    )

    consolidar_task
