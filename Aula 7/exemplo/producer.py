from time import sleep
from json import dumps
from kafka import KafkaProducer
import pandas as pd

# Cria um producer, responsavel por enviar mensagens ao Kafka
producer = KafkaProducer(bootstrap_servers=['awari-kafka:9093'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


# Envia mensagens a cada 5 segundos para o Kafka Aleatoriamente
for e in range(1000):
    data = {'number' : e}

    data = pd.read_json("https://api.exchangerate-api.com/v4/latest/USD")

    valor_brl = data.loc['BRL', 'rates']

    # producer.send('numtest', value=data.to_json(orient='records'))
    producer.send('numtest', value=valor_brl)
    sleep(1)