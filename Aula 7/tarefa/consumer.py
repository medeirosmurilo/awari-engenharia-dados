from kafka import KafkaConsumer
import os
import pandas as pd

consumer = KafkaConsumer(
    'aula-07',
    bootstrap_servers=['awari-kafka:9093'],
    group_id='tarefa-aula-07',
    enable_auto_commit=True,
    auto_offset_reset='earliest'
)

print("Consumidor iniciado. Aguardando eventos...")

for mensagem in consumer:
    caminho_arquivo = mensagem.value.decode('utf-8')
    print(f"Novo evento recebido: {caminho_arquivo}")
    
    try:
        # Lê o arquivo recém-adicionado
        novos_dados = pd.read_csv(caminho_arquivo)
        print(f"Processando arquivo: {caminho_arquivo}")

        for uf in novos_dados['codigo_uf'].unique():
            # Filtra os dados da UF
            dados_uf = novos_dados[novos_dados['codigo_uf'] == uf]

            # Nome da pasta e do arquivo CSV correspondente
            uf_pasta = f"/home/awari/app/aula-07/tarefa/municipios-estados/csv/{uf}"
            uf_arquivo = os.path.join(uf_pasta, 'cidades.csv')

            # Cria a pasta se não existir
            os.makedirs(uf_pasta, exist_ok=True)

            # Lê os dados existentes ou cria um novo DataFrame
            if os.path.exists(uf_arquivo):
                df_existente = pd.read_csv(uf_arquivo)
                df_final = pd.concat([df_existente, dados_uf], ignore_index=True)
            else:
                df_final = dados_uf

            # Salva o arquivo atualizado
            df_final.to_csv(uf_arquivo, index=False)
            print(f"Dados adicionados ao arquivo: {uf_arquivo}")

    except Exception as e:
        print(f"Erro ao processar arquivo {caminho_arquivo}: {e}")