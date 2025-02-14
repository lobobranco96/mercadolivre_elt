import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage
import requests
from bs4 import BeautifulSoup
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class MercadoLivre:
    """
    Classe para coletar dados de produtos do Mercado Livre utilizando scraping.
    """

    def __init__(self, url, headers=None):
        """
        Inicializa a classe MercadoLivre com a URL da página de produtos e os headers para a requisição HTTP.

        Args:
            url (str): URL da página do Mercado Livre para buscar os produtos.
            headers (dict, optional): Cabeçalhos HTTP personalizados para a requisição. Se None, será usado um cabeçalho padrão.
        """
        self.url = url
        self.headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    def generate_url_product(self, url):
        """
        Gera uma lista de URLs de produtos a partir da página do Mercado Livre.

        Args:
            url (str): URL da página de listagem de produtos.

        Returns:
            tuple: Lista de URLs de produtos e o objeto BeautifulSoup da página.
        """
        response = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(response.text, "html.parser")
        lista_url = []

        main = soup.find_all(class_="ui-search-layout__item") 
        for pagina in main:
            a_tag = pagina.find('a', class_='poly-component__title')
            if a_tag and 'href' in a_tag.attrs:
                href_value = a_tag['href']
                lista_url.append(href_value)

        return lista_url, soup


    def fetch_data(self, url):
        """
        Coleta dados detalhados de um produto a partir da URL do Mercado Livre.

        Args:
            url (str): URL de um produto específico.

        Returns:
            dict: Dicionário com os dados do produto, ou None se ocorrer erro durante a requisição.
        """
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")
            main = soup.find(class_="ui-pdp-container__row ui-pdp-component-list pr-16 pl-16")

            product_name = "Não encontrado"  # Valor default caso não encontre a tag

            # Verifica se 'main' não é None antes de tentar buscar a tag
            if main:
                product_name_tag = main.find("h1", class_="ui-pdp-title")
                if product_name_tag:
                    product_name = product_name_tag.get_text()

            marca_tag = soup.find('span', class_='ui-pdp-color--BLACK ui-pdp-size--XSMALL ui-pdp-family--SEMIBOLD')
            marca = marca_tag.get_text().strip() if marca_tag else "Não encontrado"

            parcela = "Não encontrado"  # Valor default caso não encontre a tag
            # Verifica se 'main' não é None antes de tentar buscar a tag
            if main:
              parcela_tag = main.find("p", class_="ui-pdp-color--GREEN ui-pdp-size--MEDIUM ui-pdp-family--REGULAR")
              if parcela_tag:
                  parcela = parcela_tag.get_text() if parcela_tag else "Não encontrado"

            preco = "Não encontrado"  # Valor default caso não encontre a tag
            # Verifica se 'main' não é None antes de tentar buscar a tag
            if main:
              preco_tag = main.find("span", class_="andes-money-amount ui-pdp-price__part andes-money-amount--cents-superscript andes-money-amount--compact")
              if preco_tag:
                preco = preco_tag.get_text().strip() if preco_tag else "0"

            preco_antigo = "Não encontrado"  # Valor default caso não encontre a tag
            # Verifica se 'main' não é None antes de tentar buscar a tag
            if main:
              preco_antigo_tag = main.find("span", class_="andes-money-amount__fraction")
              if preco_tag:
                preco_antigo = preco_antigo_tag.get_text() if preco_antigo_tag else "0"

            centavos = "Não encontrado"  # Valor default caso não encontre a tag
            # Verifica se 'main' não é None antes de tentar buscar a tag
            if main:
              centavos_tag = main.find("span", class_="andes-money-amount__cents andes-money-amount__cents--superscript-16")
              if centavos_tag:
                centavos = centavos_tag.get_text() if centavos_tag else "0"

            full_preco_antigo = preco_antigo + "," + centavos

            desconto = "Não encontrado"  # Valor default caso não encontre a tag
            # Verifica se 'main' não é None antes de tentar buscar a tag
            if main:
              desconto_tag = main.find("span", class_="andes-money-amount__discount ui-pdp-family--REGULAR")
              if desconto_tag:
                desconto = desconto_tag.get_text().strip() if desconto_tag else "0"

            extra = "Não encontrado"
            novo_ou_usado = "Não encontrado"
            vendidos = "Não encontrado"
            if main:
              extra_tag = main.find("div", class_="ui-pdp-header__subtitle")
              if extra_tag:
                extra = extra_tag.get_text().split(" | ") if extra_tag else ["Não encontrado", "Não encontrado"]
                extra = extra[:2]  # Garante que a lista terá no máximo 2 elementos
                novo_ou_usado = extra[0].strip() if len(extra) > 0 else "Não encontrado"
                vendidos = extra[1].strip() if len(extra) > 1 else "0"

            return {
                "nome_produto": product_name,
                "marca": marca,
                "preco_novo": preco,
                "parcela": parcela,
                "preco_antigo": full_preco_antigo,
                "desconto_percentual": desconto,
                "status": novo_ou_usado,
                "vendidos": vendidos,
                "product_url": url
            }
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro de requisição ao acessar {url}: {e}")
            return None

    def get_all_pages_data(self):
        """
        Coleta dados de todas as páginas de produtos da URL fornecida.

        Returns:
            pd.DataFrame: DataFrame contendo os dados de todos os produtos coletados.
        """
        dataframe = pd.DataFrame()
        url = self.url

        with ThreadPoolExecutor(max_workers=5) as executor:  # Ajuste o número de workers conforme necessário
            while url:
                print(f"Coletando dados da página: {url}")
                try:
                    product_urls, soup = self.generate_url_product(url)
                    # Usar o ThreadPoolExecutor para coletar dados de produtos em paralelo
                    futures = [executor.submit(self.fetch_data, product_url) for product_url in product_urls]
                    for future in as_completed(futures):
                        result = future.result()
                        if result:
                            df = pd.DataFrame([result])
                            dataframe = pd.concat([dataframe, df], ignore_index=True)
                            

                    # Verificar se há uma próxima página
                    next_button = soup.find("li", class_="andes-pagination__button andes-pagination__button--next")
                    if next_button and next_button.find('a'):
                        url = next_button.find('a')['href']
                    else:
                        break  # Se não houver "próxima página", sai do loop
                except Exception as e:
                    logging.error(f"Erro ao coletar dados da página {url}: {e}")
                    continue

        return dataframe

def upload_to_gcs(local_tmp_path, bucket_name, gcs_full_path):
    """
    Faz o upload de um arquivo local para um bucket do Google Cloud Storage (GCS).

    Args:
        local_tmp_path (str): Caminho do arquivo local a ser enviado.
        bucket_name (str): Nome do bucket GCS onde o arquivo será armazenado.
        gcs_full_path (str): Caminho completo no GCS onde o arquivo será armazenado.

    Returns:
        blob: O blob (arquivo) no GCS.
    """
    key_path = "/usr/local/airflow/dags/credentials/google_credential.json"
    client = storage.Client.from_service_account_json(key_path)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_full_path)

    logger.info(f'Arquivo enviado para {gcs_full_path}')
    return blob.upload_from_filename(local_tmp_path)



def coletar_dados_produtos(url_produto, bucket_name, gcs_full_path):
    """
    Coleta dados de um produto no Mercado Livre e faz o upload dos dados para o Google Cloud Storage.

    Args:
        url_produto (str): URL do produto no Mercado Livre.
        bucket_name (str): Nome do bucket do GCS onde os dados serão armazenados.
        gcs_full_path (str): Caminho completo no GCS onde os dados serão armazenados.
    """
    # Instanciar a classe MercadoLivreWebScraper com o produto URL
    ml = MercadoLivre(url=url_produto)

    # Coletar dados de todas as páginas
    dataframe = ml.get_all_pages_data()
    print(dataframe)
    nome_produto = url_produto.split('/')[3]

    # Salvar os dados em um arquivo CSV
    local_tmp_path = f"/tmp/{nome_produto}.csv"

    # Salvar os dados em um arquivo CSV temporário
    dataframe.to_csv(local_tmp_path, index=False)

    # Definir o caminho do arquivo no GCS (exemplo: "produtos/tenis_feminino.csv")

    upload_to_gcs(local_tmp_path, bucket_name, gcs_full_path)

    # Excluir o arquivo local após o upload
    if os.path.exists(local_tmp_path):
        os.remove(local_tmp_path)
        logger.info(f"Arquivo local {local_tmp_path} excluído após o upload.")

if __name__ == "__main__":
    # Os argumentos serão passados dentro da dag que executará a função
    coletar_dados_produtos(url_produto, bucket_name, gcs_full_path)