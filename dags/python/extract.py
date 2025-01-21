import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

class MercadoLivreWebScraper:
    def __init__(self, url, headers):
        self.url = url
        self.headers = headers

    def generate_url_product(self, url):
        response = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(response.text, "html.parser")
        lista_url = []

        # Encontrar todos os itens de produto na página de busca
        main = soup.find_all(class_="ui-search-layout__item")  # Ajuste para 'find_all'
        for pagina in main:
            a_tag = pagina.find('a', class_='poly-component__title')  # Ajuste da classe
            if a_tag and 'href' in a_tag.attrs:
                href_value = a_tag['href']
                lista_url.append(href_value)

        return lista_url, soup

    def fetch_data(self, url):
        response = requests.get(url, headers=self.headers, timeout=1)
        if response.status_code != 200:
            print(f"Status code : {response.status_code} | Erro ao acessar a URL: {url}")
            return None
        soup = BeautifulSoup(response.text, "html.parser")
        main = soup.find(class_="ui-pdp-container__row ui-pdp-component-list pr-16 pl-16")

        # Pegar as informações da página do produto
        product_name_tag = main.find("h1", class_="ui-pdp-title")
        product_name = product_name_tag.get_text() if product_name_tag else "Não encontrado"

        parcela_tag = main.find("p", class_="ui-pdp-color--GREEN ui-pdp-size--MEDIUM ui-pdp-family--REGULAR")
        parcela = parcela_tag.get_text() if parcela_tag else "Não encontrado"

        preco_tag = main.find("span", class_="andes-money-amount ui-pdp-price__part andes-money-amount--cents-superscript andes-money-amount--compact")
        preco = preco_tag.get_text().replace("R$", "") if preco_tag else "0"

        preco_antigo_tag = main.find("span", class_="andes-money-amount__fraction")
        preco_antigo = preco_antigo_tag.get_text() if preco_antigo_tag else "0"

        centavos_tag = main.find("span", class_="andes-money-amount__cents andes-money-amount__cents--superscript-16")
        centavos = centavos_tag.get_text() if centavos_tag else "0"

        full_preco_antigo = preco_antigo + "," + centavos

        desconto_tag = main.find("span", class_="andes-money-amount__discount ui-pdp-family--REGULAR")
        desconto = desconto_tag.get_text().replace("% OFF", "") if desconto_tag else "0"

        extra_tag = main.find("div", class_="ui-pdp-header__subtitle")
        extra = extra_tag.get_text().split(" | ") if extra_tag else ["Não encontrado", "Não encontrado"]

        novo_ou_usado = extra[0].strip() if len(extra) > 0 else "Não encontrado"
        vendidos = extra[1].strip().replace("+", "").replace(" vendidos", "").replace(" vendido", "").replace("mil", "000") if len(extra) > 1 else "0"

        marca_tag = soup.find('span', class_='ui-pdp-color--BLACK ui-pdp-size--XSMALL ui-pdp-family--SEMIBOLD')
        marca = marca_tag.get_text().strip() if marca_tag else "Não encontrado"

        return {
            "nome_produto": product_name,
            "marca": marca,
            "preco_novo": preco,
            "parcela": parcela,
            "preco_antigo": full_preco_antigo,
            "desconto | %": desconto,
            "status": novo_ou_usado,
            "vendidos": int(vendidos),
            "product_url": url
        }

    def get_all_pages_data(self):
        url = self.url
        dataframe = pd.DataFrame()

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
                    print(f"Erro ao coletar os dados da página: {url}, erro: {e}")
                    continue  # Caso algum erro ocorra, sai do loop

        return pd.Dataframe(dataframe).to_csv("../../include/products.csv", index=False)
    

if __name__ == "__main__":
    # Cabeçalho para evitar bloqueios
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    # URL inicial da lista de produtos
    url = "https://lista.mercadolivre.com.br/tenis-corrida-masculino"

    # Criar uma instância do scraper
    ml = MercadoLivreWebScraper(url, headers)

    # Coletar dados de todas as páginas
    ml.get_all_pages_data()