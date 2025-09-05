import mysql.connector
from mysql.connector import Error
import pandas as pd
import yfinance as yf
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException


# Configuração do banco
db_config = {
    'host': '127.0.0.1',
    'database': 'b3',
    'user': 'root',
    'password': 'vtecdohcek9'
}

def insert_to_database(data):
    """Insere dados no banco de dados MySQL (sem duplicados)"""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO cotacoes (codigo, preco, data)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
            
                preco = VALUES(preco),
                data = VALUES(data)
        """
        cursor.executemany(insert_query, data)
        conn.commit()
        print(f"{cursor.rowcount} registros inseridos/atualizados com sucesso!")

    except Error as error:
        print(f"Erro ao inserir no banco de dados: {error}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


# Configurar opções do Chrome
chrome_options = Options()
chrome_options.add_argument("--headless")  
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(options=chrome_options)

def setup_page():
    """Configura a página com 120 itens por página"""
    try:
        driver.get("https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBRA?language=pt-br")
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.table tbody tr"))
        )
        select_element = driver.find_element(By.ID, "selectPage")
        select = Select(select_element)
        select.select_by_visible_text("120")
        time.sleep(3)
        return True
    except Exception as e:
        print(f"Erro ao configurar a página: {e}")
        return False

try:
    for attempt in range(3):
        if setup_page():
            break
        elif attempt < 2:
            print("Tentando novamente...")
            time.sleep(5)
        else:
            print("Falha ao configurar a página após 3 tentativas")
            exit(1)

    acoes = []
    page_count = 1
    max_attempts = 3

    while True:
        print(f"Coletando dados da página {page_count}...")

        for attempt in range(max_attempts):
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.table tbody tr"))
                )
                codigos = driver.find_elements(By.CSS_SELECTOR, "table.table tbody tr td:first-child")
                for codigo in codigos:
                    acoes.append(codigo.text)
                break
            except (TimeoutException, WebDriverException) as e:
                print(f"Tentativa {attempt + 1} falhou: {e}")
                if attempt < max_attempts - 1:
                    driver.refresh()
                    time.sleep(5)
                    select_element = driver.find_element(By.ID, "selectPage")
                    select = Select(select_element)
                    select.select_by_visible_text("120")
                    time.sleep(3)
                else:
                    print("Falha após múltiplas tentativas. Continuando para a próxima página.")

        try:
            next_button = driver.find_element(By.CSS_SELECTOR, "li.pagination-next a")
            if "disabled" in next_button.get_attribute("class"):
                break
            next_button.click()
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.table tbody tr"))
            )
            time.sleep(2)
            page_count += 1
        except Exception as e:
            print(f"Não foi possível encontrar o botão de próxima página: {e}")
            break

    print(f"Foram encontradas {len(acoes)} ações:")
    print(acoes)

finally:
    driver.quit()


# Coletar preços e salvar no banco
tickers = acoes
preco_dados = []

for ticker in tickers:
    ticker_sa = ticker + '.SA'
    data = yf.download(ticker_sa, period="1d", interval="1m")

    if not data.empty and 'Close' in data.columns:
        ultimo_preco = data['Close'].iloc[-1].item()
        ultima_data = data.index[-1].date()
        preco_dados.append((ticker, round(ultimo_preco, 2), ultima_data))
    else:
        print(f"⚠️  Não há dados suficientes para {ticker}, ignorando...")
        # não adiciona nada à lista se os dados são inválidos

# Só insere se houver registros válidos
if preco_dados:
    insert_to_database(preco_dados)
else:
    print("Nenhum dado válido para inserir no banco.")