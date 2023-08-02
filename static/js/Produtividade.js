<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Produtividade</title>
    <link rel="stylesheet" href="/static/css/Produtividade.css"/>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.5/font/bootstrap-icons.css">
<body>
    <div class="Titulo">
        <h1>Produtividade</h1>
    </div>

    <div class="Filtros">
        <div class="DataInicio">
            <label for="text">Data Inicial:</label>
            <input id="DataInicial" type="date">
        </div>
        <div class="DataFim">
            <label for="text">Data Final:</label>
            <input id="DataFinal" type="date">
        </div>
        <div class="Botao">
            <button id="btnFiltrar">Filtrar</button>
        </div>
        <div class="Retorna">
            <div class="RetornaPcs">
            <label for="text">Retorna Pçs:</label>
            <label id="RetornaPcs" for="text"></label>
            </div>
                <div class="RetornaR$">
                <label  for="text">Retorna:</label>
                <label id="RetornaValor" for="text"></label>
                </div>
        </div>
        <div class="Faturado">
            <div class="FaturadoPcs">
                <label for="text">Retorna Mplus Pçs:</label>
                <label id="RetornaMplus" for="text"></label>
            </div>
                    <div class="FaturadoR$">
                    <label for="text">Retorna Mplus:</label>
                    <label id="RetornaMplusR$" for="text"></label>
                    </div>
        </div>
        <div class="Metas">
            <div class="MetaMes">
                <label for="text"></label>
                <label for="text"></label>
            </div>
            <div class="MetaDia">
                <label for="text"></label>
                <label for="text"></label>
            </div>
        </div>
    </div>

    <div class="Produtividade">
        <div class="ProdutividadeRepositor">
            <h2>Produtividade Reposição</h2>
            <div class="TabelaContainer">
                <div class="TabelaContainer2">
                <table border="2" id="TAbelaRepositor"></table>
                </div>
                <div class="TotalReposicao">
                    <i class="bi bi-trophy"></i>
                    <label id="NomeRecordeRep" for="text"></label>
                    <label id="ValorRecordeRep" for="text"></label>
                    <label id="LabelReposto" for="text">Total Reposto:</label>
                    <label id="LabelTotalReposto" for="text"></label>
                </div>
            </div>
        </div>
        <div class="linha"></div>
            <div class="ProdutividadeSeparador">
                <h2>Produtividade Separação</h2>
            <div class="TabelaContainer">
                <div class="TabelaContainer2">
                    <table border="2"  id="TAbelaSeparador"></table>
                </div>
                <div class="TotalSeparacao">
                    <i class="bi bi-trophy"></i>
                    <label id="NomeRecordeSep" for="text"></label>
                    <label id="ValorRecordeSep" for="text"></label>
                    <label id="LabelSeparado" for="text">Total Separado:</label>
                    <label id="LabelTotalSeparado" for="text"></label>
                </div>
            </div>
        </div>
    </div>




        
    
    
</body>
<script src="/static/js/Produtividade.js"></script>
</html>
