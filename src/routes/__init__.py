from flask import Blueprint

# Crie um Blueprint para as rotas
routes_blueprint = Blueprint('routes', __name__)

# Importe as rotas dos arquivos individuais
from .usuarios import usuarios_routes
from .inventario import inventario_routes
from .endereco import endereco_routes
from .reposicaoOP import reposicaoOP_routes
from .produtividades import produtividade_routes
from .empresa_natureza_Cad import empresa_natureza_routes
from .reposicaoSKU import reposicaoSKU_routes
from .estoqueEndereco import estoqueEndereco_routes
from .silk_WMS import silkWMS_routes
from .pedidos import pedidos_routes
from .portalWms import portal_routes


# Registre as rotas nos blueprints
routes_blueprint.register_blueprint(usuarios_routes)
routes_blueprint.register_blueprint(inventario_routes)
routes_blueprint.register_blueprint(endereco_routes)
routes_blueprint.register_blueprint(reposicaoOP_routes)
routes_blueprint.register_blueprint(produtividade_routes)
routes_blueprint.register_blueprint(empresa_natureza_routes)
routes_blueprint.register_blueprint(reposicaoSKU_routes)
routes_blueprint.register_blueprint(estoqueEndereco_routes)
routes_blueprint.register_blueprint(silkWMS_routes)
routes_blueprint.register_blueprint(pedidos_routes)
routes_blueprint.register_blueprint(portal_routes)
