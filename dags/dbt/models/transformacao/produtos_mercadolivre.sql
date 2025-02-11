{{ config(
    materialized="table"
) }}

WITH produtos_modificados AS (
    SELECT
        nome_produto,
        marca,
        -- Modificando a coluna 'preco_novo'
        REGEXP_REPLACE(
            REGEXP_REPLACE(preco_novo, 'Não encontrado', '0'),
            'R\\$', ''
        ) AS preco_novo,
        
        -- Modificando a coluna 'parcela'
        REGEXP_REPLACE(parcela, 'Não encontrado', '0') AS parcela,
        
        -- Modificando a coluna 'preco_antigo'
        REGEXP_REPLACE(
            REGEXP_REPLACE(preco_antigo, 'Não encontrado', '00'),  -- Substitui 'Não encontrado' por '0'
            '00,Não encontrado', '0'  -- Substitui '00,Não encontrado' por '0'
        ) AS preco_antigo,
        
        -- Modificando a coluna 'desconto | %'
        REGEXP_REPLACE(
            REGEXP_REPLACE(desconto_percentual, 'Não encontrado', '0'),
            '% OFF', ''
        ) AS desconto_percentual,
        
        status,
        
        -- Modificando a coluna 'vendidos'
        COALESCE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(vendidos, 'vendido', ''),
                            'mil', '000'
                        ),
                        '\\+', ''
                    ),
                    ' s', ''
                ),
            'Não encontrado', '0'
        ) AS vendidos,
        
        product_url
    FROM {{ ref('source_produtos') }}
)

SELECT
    nome_produto,
    marca,
    preco_novo,
    parcela,
    preco_antigo,
    desconto_percentual,
    status,
    vendidos,
    product_url
FROM produtos_modificados;
