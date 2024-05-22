<?php
include_once("./templates/cabecalho.php");
?>

<!DOCTYPE html>
<html lang="pt-BR">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Gráfico de Rosca</title>
    <link rel="stylesheet" href="./css/ContainerWms.css">
    <link rel="stylesheet" href="./css/Modais.css">
    <script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>
    <script src="https://cdn.jsdelivr.net/npm/apexcharts@3.29.0/dist/apexcharts.min.js"></script>
</head>

<style>
    .modalLoading {
        display: none;
        position: fixed;
        z-index: 9999;
        left: 0;
        top: 0;
        width: 100%;
        height: 100%;
        background-color: white;
    }

    .modalLoading-content {
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        background-color: white;
        padding: 20px;
        text-align: center;
        border-radius: 8px;
    }

    .loader {
        border: 4px solid #f3f3f3;
        border-radius: 50%;
        border-top: 4px solid var(--cor2);
        width: 40px;
        height: 40px;
        animation: spin 2s linear infinite;
        margin: 0 auto 20px auto;
    }

    @keyframes spin {
        0% {
            transform: rotate(0deg);
        }

        100% {
            transform: rotate(360deg);
        }
    }

    .CorpoCima,
    .CorpoBaixo {
        display: flex;
        width: 100%;
        height: 50%;
        border: 1px solid black
    }

    .BaixoLeft,
    .BaixoRight,
    .CimaLeft,
    .CimaRight {
        display: block;
        width: 50%;
        height: 100%;
        border: 1px solid black
    }

    .medidorReposicao {
        max-width: 100%;
        max-height: 100%;
        overflow: auto
    }

    .medidorReposicaoLabel {
        max-width: 100%;
        max-height: 100%;
    }
</style>

<body class="<?php echo $classe_empresa; ?>">
    <div class="Container">
        <div class="title">
            <h3>Indicadores</h3>
        </div>

        <div class="Corpo">
            <div class="CorpoCima">
                <div class="CimaLeft">
                    <div id="medidorReposicao">
                        <div id="medidorReposicaoLabel"></div>
                    </div>
                </div>
                <div class="CimaRight">
                    <div id="medidorPedidos">
                        <div id="medidorPedidosLabel"></div>
                    </div>
                </div>
            </div>
            <div class="CorpoBaixo">
                <div class="BaixoLeft">
                    <div id="medidorEnderecos">
                        <div id="medidorEnderecosLabel"></div>
                    </div>
                </div>
                <div class="BaixoRight">
                    <div id="medidorPcsDisponiveis">
                        <div id="medidorPcsDisponiveisLabel"></div>
                    </div>
                </div>

            </div>
        </div>
    </div>
    </div>

    <div class="modalLoading" id="modalLoading">
        <div class="modalLoading-content">
            <div class="loader"></div>
            <p>Aguarde, carregando...</p>
        </div>
    </div>

    <script>

        var empresa = '<?php echo $empresa; ?>';
        const ApiFilaPedidos = 'http://192.168.0.183:5000/api/RelatorioTotalFila';
        const ApiPedidosMatriz = 'http://192.168.0.183:5000/api/statuspedidos';
        const ApiEnderecosMatriz = 'http://192.168.0.183:5000/api/DisponibilidadeEnderecos?';
        const ApiFilaPedidosFilial = 'http://192.168.0.184:5000/api/RelatorioTotalFila';
        const ApiPedidosFilial = 'http://192.168.0.184:5000/api/statuspedidos';
        const ApiEnderecosFilial = 'http://192.168.0.184:5000/api/DisponibilidadeEnderecos?';

        async function fetchData(apiUrl) {
            try {
                const response = await fetch(`${apiUrl}`, {
                    method: 'GET',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': "a40016aabcx9"
                    },
                });

                if (!response.ok) {
                    throw new Error('Erro no retorno da API');
                }

                return response.json();
            } catch (error) {
                console.error(error);
                return null;
            }
        }

        async function fetchDataEnderecos(apiUrl, natureza) {
            try {
                const response = await fetch(`${apiUrl}empresa=${empresa}&natureza=${natureza}`, {
                    method: 'GET',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': "a40016aabcx9"
                    },
                });

                if (!response.ok) {
                    throw new Error('Erro no retorno da API');
                }

                return response.json();
            } catch (error) {
                console.error(error);
                return null;
            }
        }


        async function renderDonutChart(data) {
            if (data.length > 0) {
                var totalPcs = data[0]["1.1-Total de Peças Nat. 5"];
                totalPcs = totalPcs.replace(/\./, '');
                totalPcs = totalPcs.replace(/\ pçs/, '');
                const totalPcsFormatado = parseInt(totalPcs).toLocaleString('pt-BR');
                var PcsRepostas = data[0]["1.3-Peçs Repostas"];
                PcsRepostas = PcsRepostas.replace(/\./, '');
                PcsRepostas = PcsRepostas.replace(/\ pçs/, '');
                const PcsRepostasFormatado = parseInt(totalPcs).toLocaleString('pt-BR');
                const Diferenca =  parseInt(totalPcs - PcsRepostas).toLocaleString('pt-BR');

                if (!isNaN(totalPcs) && !isNaN(PcsRepostas)) {
                    const PercentualReposto = (PcsRepostas / totalPcs) * 100;
                    const porcentagemDiferenca = 100 - PercentualReposto;

                    const chartOptions = {
                        chart: {
                            type: 'donut',
                            height: '100%', // Define a altura do gráfico para 100% da div
                        },
                        title: {
                            text: 'Taxa de Peças Repostas', // Aqui está o título que você quer adicionar
                            align: 'center', // Alinhamento do título
                            style: {
                                fontSize: '20px', // Tamanho da fonte do título
                                fontWeight: 'bold', // Peso da fonte do título
                                color: '#333' // Cor do título
                            }
                        },
                        series: [PercentualReposto, porcentagemDiferenca],
                        labels: [`Total Peças Estoque: <b>${totalPcsFormatado}</b>`, `Total Peças Repostas: <b>${PcsRepostasFormatado}</b>`, `Diferença: <b>${Diferenca}</b>`],
                        colors: ['#112d7e', '#008FFB', 'red'],
                        responsive: [{
                            breakpoint: 480,
                            options: {
                                legend: {
                                    position: 'bottom'
                                }
                            }
                        }],
                        plotOptions: {
                            pie: {
                                donut: {
                                    labels: {
                                        show: true,
                                        name: {
                                            show: true
                                        },
                                        value: {
                                            show: true,
                                            formatter: function (val) {
                                                return val.toFixed(2) + "%"; // Mostra o valor com duas casas decimais
                                            }
                                        },
                                        total: {
                                            show: true,
                                            formatter: function () {
                                                return PercentualReposto.toFixed(2) + '%'; // Exibe o percentual de segunda qualidade no centro do donut
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        dataLabels: {
                            enabled: false
                        },
                        legend: {
                            show: true,
                            position: 'bottom',
                            fontSize: '15px'
                        }
                    };

                    const chart = new ApexCharts(document.querySelector("#medidorReposicao"), chartOptions);
                    chart.render();
                } else {
                    throw new Error('Valores inválidos para criar o gráfico');
                }
            } else {
                throw new Error('Nenhum dado disponível para criar o gráfico');
            }
        }


        async function renderDonutChart2(data) {
            if (data.length > 0) {
                var totalPedidosRetorna = data[0]["1. Total de Pedidos no Retorna"];
                totalPedidosRetorna = totalPedidosRetorna.replace(/\./, '');
                totalPedidosRetorna = totalPedidosRetorna.replace(/\ pçs/, '');
                const totalPedidosRetornaFormatado = parseInt(totalPedidosRetorna).toLocaleString('pt-BR');
                var PedidosFecham = data[0]["2. Total de Pedidos fecham 100%"];
                PedidosFecham = PedidosFecham.replace(/\./, '');
                PedidosFecham = PedidosFecham.replace(/\ pçs/, '');
                const PedidosFechamFormatado = parseInt(PedidosFecham).toLocaleString('pt-BR');
                const Diferenca = parseInt(totalPedidosRetorna - PedidosFecham).toLocaleString('pt-BR');

                if (!isNaN(totalPedidosRetorna) && !isNaN(PedidosFecham)) {
                    const PercentualPedidosFecham = (PedidosFecham / totalPedidosRetorna) * 100;
                    const porcentagemDiferenca = 100 - PercentualPedidosFecham;

                    const chartOptions = {
                        chart: {
                            type: 'donut',
                            height: '100%',
                        },
                        title: {
                            text: 'Taxa de Pedidos 100%',
                            align: 'center',
                            style: {
                                fontSize: '20px',
                                fontWeight: 'bold',
                                color: '#333'
                            }
                        },
                        series: [PercentualPedidosFecham, porcentagemDiferenca],
                        labels: [`Pedidos No Retorna: <b>${totalPedidosRetornaFormatado}</b>`, `Pedidos 100% Repostos: <b>${PedidosFechamFormatado}</b>`, `Diferença: <b>${Diferenca}</b>`],
                        colors: ['#112d7e', '#008FFB', 'red'],
                        responsive: [{
                            breakpoint: 480,
                            options: {
                                legend: {
                                    position: 'bottom'
                                }
                            }
                        }],
                        plotOptions: {
                            pie: {
                                donut: {
                                    labels: {
                                        show: true,
                                        name: {
                                            show: true
                                        },
                                        value: {
                                            show: true,
                                            formatter: function (val) {
                                                return val.toFixed(2) + "%"; // Mostra o valor com duas casas decimais
                                            }
                                        },
                                        total: {
                                            show: true,
                                            formatter: function () {
                                                return PercentualPedidosFecham.toFixed(2) + '%'; // Exibe o percentual de segunda qualidade no centro do donut
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        dataLabels: {
                            enabled: false
                        },
                        legend: {
                            show: true,
                            position: 'bottom',
                            fontSize: '15px'
                        }
                    };

                    const chart = new ApexCharts(document.querySelector("#medidorPedidos"), chartOptions);
                    chart.render();
                } else {
                    throw new Error('Valores inválidos para criar o gráfico');
                }
            } else {
                throw new Error('Nenhum dado disponível para criar o gráfico');
            }
        }


        async function renderDonutChart3(data) {
            if (data.length > 0) {
                var totalEnderecos = data[0]["1- Total de Enderecos Natureza "];
                totalEnderecos = totalEnderecos.replace(/\./, '');
                totalEnderecos = totalEnderecos.replace(/\ pçs/, '');
                const totalEnderecosFormatado = parseInt(totalEnderecos).toLocaleString('pt-BR');
                var totalEnderecosOcupados = data[0]["2- Total de Enderecos Disponiveis"];
                totalEnderecosOcupados = totalEnderecosOcupados.replace(/\./, '');
                totalEnderecosOcupados = totalEnderecosOcupados.replace(/\ pçs/, '');
                totalEnderecosOcupados = totalEnderecos - totalEnderecosOcupados;
                const totalEnderecosOcupadosFormatado = parseInt(totalEnderecosOcupados).toLocaleString('pt-BR');
                var Diferenca = data[0]["2- Total de Enderecos Disponiveis"]
                Diferenca = Diferenca.replace(/\./, '');
                Diferenca = Diferenca.replace(/\ pçs/, '');
                const DiferencaFormatado = parseInt(Diferenca).toLocaleString('pt-BR');

                if (!isNaN(totalEnderecos) && !isNaN(totalEnderecosOcupados)) {
                    const PercentualEnderecosOcupados = (totalEnderecosOcupados / totalEnderecos) * 100;
                    const porcentagemDiferenca = 100 - PercentualEnderecosOcupados;

                    const chartOptions = {
                        chart: {
                            type: 'donut',
                            height: '100%', // Define a altura do gráfico para 100% da div
                        },
                        title: {
                            text: 'Taxa de Ocupação de Endereços', // Aqui está o título que você quer adicionar
                            align: 'center', // Alinhamento do título
                            style: {
                                fontSize: '20px', // Tamanho da fonte do título
                                fontWeight: 'bold', // Peso da fonte do título
                                color: '#333' // Cor do título
                            }
                        },
                        series: [PercentualEnderecosOcupados, porcentagemDiferenca],
                        labels: [`Enderecos Totais: <b>${totalEnderecosFormatado}</b>`, `Enderecos Ocupados: <b>${totalEnderecosOcupadosFormatado}</b>`, `Diferença: <b>${DiferencaFormatado}</b>`],
                        colors: ['#112d7e', '#008FFB', 'red'],
                        responsive: [{
                            breakpoint: 480,
                            options: {
                                legend: {
                                    position: 'bottom'
                                }
                            }
                        }],
                        plotOptions: {
                            pie: {
                                donut: {
                                    labels: {
                                        show: true,
                                        name: {
                                            show: true
                                        },
                                        value: {
                                            show: true,
                                            formatter: function (val) {
                                                return val.toFixed(2) + "%"; // Mostra o valor com duas casas decimais
                                            }
                                        },
                                        total: {
                                            show: true,
                                            formatter: function () {
                                                return PercentualEnderecosOcupados.toFixed(2) + '%'; // Exibe o percentual de segunda qualidade no centro do donut
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        dataLabels: {
                            enabled: false
                        },
                        legend: {
                            show: true,
                            position: 'bottom',
                            fontSize: '15px'
                        }
                    };

                    const chart = new ApexCharts(document.querySelector("#medidorEnderecos"), chartOptions);
                    chart.render();
                } else {
                    throw new Error('Valores inválidos para criar o gráfico');
                }
            } else {
                throw new Error('Nenhum dado disponível para criar o gráfico');
            }
        }


        async function renderDonutChart4(data) {
            if (data.length > 0) {
                var totalPcsPedidos = data[0]["2.1- Total de Skus nos Pedidos em aberto "];
                totalPcsPedidos = totalPcsPedidos.replace(/\./, '');
                totalPcsPedidos = totalPcsPedidos.replace(/\ pçs/, '');
                const totalPcsPedidosFormatado = parseInt(totalPcsPedidos).toLocaleString('pt-BR');
                var PcsRepostas = data[0]["2.3-Qtd de Enderecos OK Reposto nos Pedido"];
                PcsRepostas = PcsRepostas.replace(/\./, '');
                PcsRepostas = PcsRepostas.replace(/\ pçs/, '');
                const PcsRepostasFormatado = parseInt(PcsRepostas).toLocaleString('pt-BR');
                const Diferenca = parseInt(data[0]["2.2-Qtd de Enderecos Nao Reposto em Pedido"]).toLocaleString('pt-BR');

                if (!isNaN(totalPcsPedidos) && !isNaN(PcsRepostas)) {
                    const PercentualReposto = (PcsRepostas / totalPcsPedidos) * 100;
                    const porcentagemDiferenca = 100 - PercentualReposto;

                    const chartOptions = {
                        chart: {
                            type: 'donut',
                            height: '100%', // Define a altura do gráfico para 100% da div
                        },
                        title: {
                            text: 'Taxa de Peças Disponíveis para Separação', // Aqui está o título que você quer adicionar
                            align: 'center', // Alinhamento do título
                            style: {
                                fontSize: '20px', // Tamanho da fonte do título
                                fontWeight: 'bold', // Peso da fonte do título
                                color: '#333' // Cor do título
                            }
                        },
                        series: [PercentualReposto, porcentagemDiferenca],
                        labels: [`Total de Peças nos Pedidos: <b>${totalPcsPedidosFormatado}</b>`, `Total de Peças Repostas: <b>${PcsRepostasFormatado}</b>`, `Diferença: ${Diferenca}`],
                        colors: ['#112d7e', '#008FFB', 'red'],
                        responsive: [{
                            breakpoint: 480,
                            options: {
                                legend: {
                                    position: 'bottom'
                                }
                            }
                        }],
                        plotOptions: {
                            pie: {
                                donut: {
                                    labels: {
                                        show: true,
                                        name: {
                                            show: true
                                        },
                                        value: {
                                            show: true,
                                            formatter: function (val) {
                                                return val.toFixed(2) + "%"; // Mostra o valor com duas casas decimais
                                            }
                                        },
                                        total: {
                                            show: true,
                                            formatter: function () {
                                                return PercentualReposto.toFixed(2) + '%'; // Exibe o percentual de segunda qualidade no centro do donut
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        dataLabels: {
                            enabled: false
                        },
                        legend: {
                            show: true,
                            position: 'bottom',
                            fontSize: '15px'
                        }
                    };

                    const chart = new ApexCharts(document.querySelector("#medidorPcsDisponiveis"), chartOptions);
                    chart.render();
                } else {
                    throw new Error('Valores inválidos para criar o gráfico');
                }
            } else {
                throw new Error('Nenhum dado disponível para criar o gráfico');
            }
        }



        async function initializeCharts() {
    // Limpa o conteúdo dos elementos onde os gráficos serão renderizados
    document.getElementById("medidorReposicao").innerHTML = '';
    document.getElementById("medidorPedidos").innerHTML = '';
    document.getElementById("medidorEnderecos").innerHTML = '';
    document.getElementById("medidorPcsDisponiveis").innerHTML = '';

    const apiFilaPedidos = empresa === "1" ? ApiFilaPedidos : ApiFilaPedidosFilial;
    const apiPedidos = empresa === "1" ? ApiPedidosMatriz : ApiPedidosFilial;
    const apiEnderecos = empresa === "1" ? ApiEnderecosMatriz : ApiEnderecosFilial;


    const dataReposicao = await fetchData(apiFilaPedidos);
    if (dataReposicao) {
        renderDonutChart(dataReposicao);
    }

    const dataPedidos = await fetchData(apiPedidos);
    if (dataPedidos) {
        renderDonutChart2(dataPedidos);
    }

    const dataEnderecos = await fetchDataEnderecos(apiEnderecos, "5");
    if (dataEnderecos) {
        renderDonutChart3(dataEnderecos);
    }

    const dataPecas = await fetchData(apiFilaPedidos);
    if (dataPecas) {
        renderDonutChart4(dataPecas);
    }
}





        document.addEventListener("DOMContentLoaded", function () {

            initializeCharts(); // Inicia os gráficos com os dados da data de hoje
        });
    </script>
</body>

</html>