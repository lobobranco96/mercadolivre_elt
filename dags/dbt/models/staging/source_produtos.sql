{{ config(
    materialized="view"
) }}

SELECT
    nome_produto AS nome_produto,
    marca AS marca,
    preco_novo AS preco_novo,
    parcela AS parcela,
    preco_antigo AS preco_antigo,
    desconto | % AS desconto_porcentagem,
    status AS status,
    vendidos AS vendidos,
    product_url AS url_produto
FROM {{ source('mercadolivre', 'produtos') }}
