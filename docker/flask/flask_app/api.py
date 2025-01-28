from flask import Flask, jsonify, request
from tinydb import TinyDB, Query
import datetime

# Inicializa o servidor Flask e o banco de dados TinyDB
server = Flask(__name__)
db = TinyDB('database.json')

# Rota principal da API
@server.route('/api/produtos', methods=['GET'])
def produtos():
    # Coleta todos os produtos no banco de dados
    produtos = db.all()

    if produtos:
        return jsonify(produtos)  # Retorna todos os produtos armazenados no banco
    else:
        return jsonify({"message": "Nenhum produto encontrado."}), 404

# Rota para adicionar novos produtos via POST (Usuário envia dados)
@server.route('/api/produtos', methods=['POST'])
def add_produto():
    data = request.get_json()

    # Validação simples
    if "produto" not in data:
        return jsonify({"message": "O campo 'produto' é obrigatório!"}), 400

    # Criando o objeto do produto
    produto = {
        "produto": data["produto"],
        "data_insercao": datetime.datetime.now().strftime("%Y-%m-%d")
    }

    # Inserindo no TinyDB
    db.insert(produto)

    # Retornando o produto adicionado junto com a mensagem de sucesso
    return jsonify({
        "message": "Produto adicionado com sucesso!",
        "produto": produto["produto"],
        "data_insercao": produto["data_insercao"]
    }), 200

# Rota para recuperar todos os produtos (funcionário consulta os dados via GET)
@server.route('/api/produtos/all', methods=['GET'])
def get_all_data():
    data = db.all()  # Recupera todos os dados armazenados
    if data:
        return jsonify(data)
    else:
        return jsonify({"message": "Nenhum produto encontrado"}), 404

# Rota para buscar produtos com base na data de inserção (exemplo: "2025-01-24")
@server.route('/api/produtos/all/date/<date>', methods=['GET'])
def get_data_by_date(date):
    produto = Query()

    # Consulta no banco de dados para buscar as entradas com a data de inserção fornecida
    dados = db.search(produto.data_insercao == date)  # Filtra por data (formato YYYY-MM-DD)

    if dados:
        return jsonify(dados)
    else:
        return jsonify({'message': 'Nenhum dado encontrado para a data especificada'}), 404

if __name__ == '__main__':
   server.run(host='0.0.0.0', port=5000)