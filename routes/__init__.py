############# NESSE ARQUIVO É INSERIDO TODOS OS GRUPOS DE  ROTAS DE API's ,
###### organizado por arquivos presente na subpasta "/routes" do projeto.
##### trata-se de uma forma de orgranizar o sistema de forma intuitiva e didatica para manutencoes e ampliacoes.

from flask import Blueprint

# Cria um Blueprint para as rotas
routes_blueprint = Blueprint('routes', __name__)

# Importe as rotas dos arquivos individuais
from .Usuario.usuarios import usuarios_routes
from .usuariosPortal import usuariosPortal_routes
from .linhasPortal import linhas_routes
from .inventario import inventario_routes
from .endereco import endereco_routes
from .ReposicaoOP.reposicaoOP import reposicaoOP_routes
from .produtividades import produtividade_routes
from .empresa_natureza_Cad import empresa_natureza_routes
from .reposicaoSKU import reposicaoSKU_routes
from .estoqueEndereco import estoqueEndereco_routes
from .silk_WMS import silkWMS_routes
from .SeparacaoPedidos.pedidos import pedidos_routes
from .portalWms import portal_routes
from .faturamento import faturamento_routes
from .finalizacaoPedido import finalizacaoPedido_route
from .necessidadeReposicao import necessidadeRepos_routes
from .Dashbord.dashboard import dashboard_routes
from .chamados import chamados_routes
from .automacaoWMS_CSW import AutomacaoWMS_CSW_routes
from .ReposicaoQualidade_routes import reposicao_qualidadeRoute
from .AcompanhamentoQualidade import AcompanhamentoQual_routes
from .SkusSubstitutos_routes import SkusSubstitutos_routes
from .SeparacaoPedidos.pedidoApontamento import pedidosApontamento_routes
from .Dashbord.DetalhamentoFila import dashboardFila_routes
from .AutomacaoCsw.AtualizaFilaTags import AtualizaFilaTags_routes
from .AutomacaoCsw.DetalhamentoServicos import DetalhamentoServicos_routes
from .AutomacaoCsw.ReservaPreFat import ReservaPreFaturamento_routes
from  .AutomacaoCsw.AtualizaSku import InformacosPCPServicos_routes
from .AutomacaoCsw.SubstitutosSku import AtualizaSubstitutosSku_routes
from .ConfiguracaoRevisaoAPI import ConfRevisao_routes
from .ReposicaoViaOFFApi import ReposicaoViaOFF_routes
from .CarrinhoOFFApi import CarrinhoOFF_routes
from .TelaAcessoApi import PerfilTelaAcesso_routes


# Registre as rotas nos blueprints
routes_blueprint.register_blueprint(usuarios_routes)
routes_blueprint.register_blueprint(usuariosPortal_routes)
routes_blueprint.register_blueprint(linhas_routes)
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
routes_blueprint.register_blueprint(faturamento_routes)
routes_blueprint.register_blueprint(finalizacaoPedido_route)
routes_blueprint.register_blueprint(necessidadeRepos_routes)
routes_blueprint.register_blueprint(dashboard_routes)
routes_blueprint.register_blueprint(chamados_routes)
routes_blueprint.register_blueprint(AutomacaoWMS_CSW_routes)
routes_blueprint.register_blueprint(reposicao_qualidadeRoute)
routes_blueprint.register_blueprint(AcompanhamentoQual_routes)
routes_blueprint.register_blueprint(SkusSubstitutos_routes)
routes_blueprint.register_blueprint(pedidosApontamento_routes)
routes_blueprint.register_blueprint(dashboardFila_routes)
routes_blueprint.register_blueprint(AtualizaFilaTags_routes)
routes_blueprint.register_blueprint(DetalhamentoServicos_routes)
routes_blueprint.register_blueprint(ReservaPreFaturamento_routes)
routes_blueprint.register_blueprint(InformacosPCPServicos_routes)
routes_blueprint.register_blueprint(AtualizaSubstitutosSku_routes)
routes_blueprint.register_blueprint(ConfRevisao_routes)
routes_blueprint.register_blueprint(ReposicaoViaOFF_routes)
routes_blueprint.register_blueprint(CarrinhoOFF_routes)
routes_blueprint.register_blueprint(PerfilTelaAcesso_routes)