from flask import Flask
from flask_restful import Api, Resource
from app.apis import clientes
from app.apis import produtos

app = Flask(__name__)
api = Api(app)

class HomePage(Resource):

    def get(self):
        return "Home Page"


class Clientes(Resource):
    
    def get(self):
        return clientes.get()

    def post(self):
        return clientes.post()

    def delete(self):
        return clientes.delete()


class Produtos(Resource):

    def get(self):
        return produtos.get()

    def post(self):
        return produtos.post()


api.add_resource(HomePage, "/")
api.add_resource(Clientes, "/clientes")
api.add_resource(Produtos, "/produtos")

if __name__ == "__main__":
    pass