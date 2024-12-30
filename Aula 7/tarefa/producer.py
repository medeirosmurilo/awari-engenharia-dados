from kafka import KafkaProducer
import os
import time

producer = KafkaProducer(bootstrap_servers=['awari-kafka:9093'])
pasta_monitorada = "/home/awari/app/aula-07/tarefa/municipios-estados/streaming/"

print(f"Monitorando a pasta: {pasta_monitorada}")
arquivos_vistos = set(os.listdir(pasta_monitorada))

while True:
    arquivos_atual = set(os.listdir(pasta_monitorada))
    novos_arquivos = arquivos_atual - arquivos_vistos

    for arquivo in novos_arquivos:
        caminho_completo = os.path.join(pasta_monitorada, arquivo)
        if os.path.isfile(caminho_completo) and arquivo.endswith('.csv'):
            topico = 'aula-07'
            mensagem = caminho_completo

            producer.send(topico, mensagem.encode('utf-8'))
            producer.flush()
            print(f"Evento enviado: {mensagem}")

    arquivos_vistos = arquivos_atual
    time.sleep(5)