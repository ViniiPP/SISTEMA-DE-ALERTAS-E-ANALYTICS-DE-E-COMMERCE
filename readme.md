# Desenvolvedor
 - Vinicius Pereira Polli


# Sistema de Alertas e Analytics de E-commerce

Este repositório contém um projeto prático desenvolvido para demonstrar o funcionamento de um fluxo completo de Engenharia de Dados, integrando processamento em tempo real (streaming) com processamento em lotes (batch). 

A ideia aqui é simular um e-commerce que não para de gerar vendas. O sistema precisa receber essas vendas, guardar tudo como histórico, detectar na hora se uma compra tem valor muito alto (possível fraude) e, de tempos em tempos, calcular o faturamento de cada categoria de produto.

## Tecnologias que usei
- **Docker e Docker Compose:** Para subir todos os bancos e serviços sem sujar a máquina.
- **Python:** Linguagem principal para gerar os dados falsos e consumir as mensagens.
- **Apache Kafka:** Funciona como o "carteiro" do projeto, recebendo todas as compras e distribuindo.
- **MongoDB:** Nosso Data Lake. Guarda as transações cruas do jeito que chegam.
- **Apache Flink:** Fica lendo o Kafka em tempo real para detectar as compras acima de R$ 500,00.
- **Redis (Valkey):** Recebe os alertas de fraude que o Flink encontra (pois é muito rápido para buscas).
- **Apache Airflow:** O maestro. Roda de tempos em tempos para processar o consolidado.
- **PostgreSQL:** Onde o Airflow salva o faturamento já somadinho por categoria.

## Como o fluxo funciona na prática
1. Um script em Python fica gerando dados falsos de compras (id do cliente, valor, categoria, etc) e jogando no tópico `vendas-raw` do **Kafka**.
2. Ao mesmo tempo, outro script em Python fica lendo esse tópico e salvando tudo direto no **MongoDB** (sem mexer nos dados, puro histórico).
3. Um job do **Apache Flink** fica "escutando" esse mesmo tópico do Kafka. Se ele vê uma transação com valor maior que 500 reais, ele grita "FRAUDE!" e salva essa transação lá no **Redis**.
4. Depois, entra o **Airflow**. Ele tem uma DAG configurada que roda de tempo em tempo, vai até o MongoDB, soma quanto cada categoria vendeu e salva esse consolidado no **PostgreSQL** para o pessoal de negócios conseguir montar dashboards.

## Como rodar o projeto

### 1. Subir a infraestrutura
Certifique-se de ter o Docker instalado e rode no terminal, dentro da pasta do projeto:
```bash
docker-compose up -d
```
Isso vai baixar as imagens e subir o Kafka, Postgres, Mongo, Redis, Flink e Airflow.

### 2. Rodar o Consumidor do Mongo
Em um terminal separado, rode o consumidor usando a imagem do Python (assim você não precisa instalar bibliotecas localmente):
```bash
docker run --rm --name mongo_consumer --network sistema-de-alertas-e-analytics-de-e-commerce_ecom_network -v "$PWD/scripts:/scripts" python:3.10 bash -c "pip install kafka-python pymongo && python /scripts/mongo_consumer.py"
```

### 3. Rodar o Produtor de Vendas (Simulador)
Abra outro terminal e rode o produtor para começar a enviar os dados:
```bash
docker run --rm --name producer --network sistema-de-alertas-e-analytics-de-e-commerce_ecom_network -v "$PWD/scripts:/scripts" python:3.10 bash -c "pip install kafka-python faker && python /scripts/producer.py"
```

### 4. Submeter o Job do Flink (Alertas)
Em um terceiro terminal, copie o script do Flink para o contêiner e execute:
```bash
docker cp scripts/flink_fraud_alert.py flink-jobmanager:/opt/flink/
docker exec -it flink-jobmanager flink run -py /opt/flink/flink_fraud_alert.py
```

### 5. Ativar a DAG no Airflow
- Acesse a interface do Airflow no navegador em `http://localhost:8086` (usuário: admin, senha: admin).
- Procure a DAG `consolidate_sales_dag` e clique no botão de "play" ou apenas ative a chavinha para ela rodar.
- Depois de verde, você pode ver o resultado no **PgAdmin** acessando `http://localhost:8084` e fazendo um `SELECT * FROM faturamento_categoria;` no banco `bigdata`.

## Evidências
Na pasta `outputs` do repositório eu deixei as imagens de cada etapa rodando (Kafka recebendo dados, Mongo guardando os documentos, Flink rodando em streaming, Redis com os alertas e o banco relacional atualizado pelo Airflow).

---
*Feito para aplicar na prática conceitos de Engenharia de Dados e mensageria - Atitus Educação*
