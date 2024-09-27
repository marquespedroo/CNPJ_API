import sqlite3
import pandas as pd 
import re
import requests
from requests.structures import CaseInsensitiveDict
import time
from datetime import datetime


# Configurações
API_URL = "https://publica.cnpj.ws/cnpj/{cnpj}"
DB_NAME = "cnpj_data.db"
EXCEL_FILE = "CNPJ_busca.xlsx"
REQUESTS_PER_MINUTE = 3
LOG_FILE = "error_log.txt"

# Remove todos os caracteres não numéricos do CNPJ
def clean_cnpj(cnpj):
    return re.sub(r'\D', '', str(cnpj))

# Cria um log de erros
def log_error(cnpj, error):
    with open(LOG_FILE, 'a') as f:
        timestamp = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        f.write(f"{timestamp} - Erro ao processar CNPJ {cnpj}: {error}\n")


# Cria o banco de dados
# Os dados que não aparecem no banco de dados não foram fornecidas pela API. Ex. coluna 'NOME'
def create_table():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cnpj_info (
        cnpj TEXT PRIMARY KEY,
        inscricao_estadual TEXT,
        razao_social TEXT,
        nome TEXT,
        nome_fantasia TEXT,
        logradouro TEXT,
        cep TEXT,
        uf TEXT
    )
    ''')
    conn.commit()
    conn.close()
    print("Tabela criada com sucesso.")

def fetch_cnpj_data(cnpj):
    url = API_URL.format(cnpj=cnpj)

    
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        estabelecimento = data.get('estabelecimento', {})

        inscricoes_estaduais = estabelecimento.get('inscricoes_estaduais', [])
        inscricao_estadual = inscricoes_estaduais[0].get('inscricao_estadual', '') if inscricoes_estaduais else ''
        
        
        return {
            'cnpj': estabelecimento.get('cnpj', ''),
            'inscricao_estadual': inscricao_estadual,
            'razao_social': data.get('razao_social', ''),
            'nome': data.get('nome', ''),
            'nome_fantasia': estabelecimento.get('nome_fantasia', ''),
            'logradouro': estabelecimento.get('logradouro', ''),
            'cep': estabelecimento.get('cep', ''),
            'uf': estabelecimento.get('estado', {}).get('sigla', '')
        }
    else:
        print(f"Erro ao consultar CNPJ {cnpj}: {response.status_code}")
        return None


# Placeholders previnem contra ataques de SQL injections
# Replace atualiza os dados de um CNPJ que já estiver no banco de dados
def insert_data(data):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
        INSERT OR REPLACE INTO cnpj_info 
        (cnpj, inscricao_estadual, razao_social, nome, nome_fantasia, logradouro, cep, uf)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data['cnpj'],
            data['inscricao_estadual'],
            data['razao_social'],
            data['nome'],
            data['nome_fantasia'],
            data['logradouro'],
            data['cep'],
            data['uf']
        ))
        conn.commit()
        print(f"Dados inseridos com sucesso para o CNPJ {data['cnpj']}")
    except Exception as e:
        print(f"Erro ao inserir dados: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

# Verifica se os dados foram inseridos corretamente no banco de dados
def verify_data_in_db(cnpj):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cnpj_info WHERE cnpj = ?", (cnpj,))
    result = cursor.fetchone()
    conn.close()
    if result:
        print(f"Dados verificados no banco para o CNPJ {cnpj}: {result}")
    else:
        print(f"Nenhum dado encontrado no banco para o CNPJ {cnpj}")



def main():
    create_table()


    df = pd.read_excel(EXCEL_FILE)
    df['CNPJ'] = df['CNPJ'].apply(clean_cnpj) # Aplica a função que trata os CNPJs.
    cnpjs = df['CNPJ'].tolist() # Faz com que a coluna vire uma lista
    
    for i, cnpj in enumerate(cnpjs): # Itera sobre a lista
        print(f"Processando CNPJ {i+1}/{len(cnpjs)}: {cnpj}")
        try:
            data = fetch_cnpj_data(cnpj)
            if data:
                insert_data(data)
                verify_data_in_db(cnpj)
            else:
                log_error(cnpj, "Dados não retornados pela API")
            
            # Respeita o limite de 3 requisições por minuto
            if (i + 1) % REQUESTS_PER_MINUTE == 0:
                print('Buscando dados...')
                time.sleep(60)  # Espera 60 segundos após cada 3 requisições
        
        except Exception as e:
            print(f"Erro ao processar CNPJ {cnpj}: {str(e)}")
            log_error(cnpj, str(e))
    
    print("Processamento concluído.")

if __name__ == "__main__":
    main()


