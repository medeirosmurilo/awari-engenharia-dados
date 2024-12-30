import os
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class NewFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            print(f"Novo arquivo detectado: {event.src_path}")
            processar_arquivo(event.src_path)


def processar_arquivo(caminho_arquivo):
    try:
        # Lê o arquivo recém-adicionado
        novos_dados = pd.read_csv(caminho_arquivo)
        print(f"Processando arquivo: {caminho_arquivo}")

        for uf in novos_dados['codigo_uf'].unique():
            # Filtra os dados da UF
            dados_uf = novos_dados[novos_dados['codigo_uf'] == uf]

            # Nome da pasta e do arquivo CSV correspondente
            uf_pasta = f"./{uf}"
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


def monitorar_pasta(pasta):
    event_handler = NewFileHandler()
    observer = Observer()
    observer.schedule(event_handler, pasta, recursive=False)
    observer.start()
    print(f"Monitorando a pasta: {pasta}")

    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


# Monitorar a pasta especificada
if __name__ == "__main__":
    monitorar_pasta("C:/Users/mmuri/Documents/Awari/Engenharia_Dados/awari-engenharia-de-dados-docker/jupyter-notebooks/Python-Basics/Aula 7/municipios-estados/streaming/")