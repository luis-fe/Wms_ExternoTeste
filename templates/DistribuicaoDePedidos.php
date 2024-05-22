<?php
include_once("./templates/cabecalho.php");
?>

<!DOCTYPE html>
<html lang="pt-BR">

<head>
    <link rel="stylesheet" href="./css/ContainerWms.css">
    <link rel="stylesheet" href="./css/Modais.css">
    <link rel="stylesheet" href="./css/Distribuicao.css">
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
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
      }

      .Dados{
        margin-top: 30px
      }

      .Dados ul{
        margin-bottom: 15px;
      }
</style>

<body class="<?php echo $classe_empresa; ?>">
    <div class="Container">
        <div class="title">
            <h3>DISTRIBUIÇÃO DE PEDIDOS</h3>
            <i class="bi bi-x-square-fill" title="Fechar" id="FecharRotina" style="color: black"></i>
        </div>

        <div class="Corpo">
            <div class="Dados">
                <select id="Usuarios" onchange="if (this.value !== 'Selecione um usuário para Atribuição!')">
                    <option disabled selected>Selecione um usuário para Atribuição!</option>
                </select>
                <button id="VerificarPecasFaltando">Peças Faltantes</button>
                <button id="Priorizacao">Priorizar Pedido</button>
                <button id="Atualizar">Atualizar Dados</button>
            </div>
            <div class="tabela" id="DivTabelaDistribuicao">
                <table id="TabelaDistribuicao" class="Tabela">
                    <thead>
                        <tr>
                            <th><input type="checkbox" id="selectAllCheckbox"></th>
                            <th>Pedido</th>
                            <th>Usuário Atribuído</th>
                            <th>Tipo de Nota</th>
                            <th>Data de Sugestão</th>
                            <th>Quantidade Peças</th>
                            <th>% Reposto</th>
                            <th>% Separado</th>
                            <th>Valor R$</th>
                            <th>Pedidos Agrupados</th>
                            <th>Estado</th>
                            <th>Situacao Pedido</th>
                            <th>Marca</th>
                            <th>Prioridade</th>
                            <th>Transportadora</th>
                        </tr>
                    </thead>
                    <tbody id="tBodyTabela">
                    </tbody>
                </table>
            </div>
            <div class="tabela" id="DivTabelaMediaPecas" style="display: none">
                <table id="TabelaMediaPecas" class="Tabela">
                    <thead>
                        <tr>
                            <th>Nome</th>
                            <th>Qtd. Pedidos</th>
                            <th>Qtd. Pecas</th>
                            <th>Média Peças</th>
                            <th>Valor R$</th>
                        </tr>
                    </thead>
                    <tbody id="tBodyTabela">
                    </tbody>
                </table>
            </div>
            <div class="botoes">
                <i id="Informacoes" class="bi bi-info-circle-fill" title="Informações de Médias" style="cursor: pointer"></i>
                <i id="Voltar" class="bi bi-arrow-left" title="Voltar" style="display: none; cursor: pointer"></i>
            </div>
        </div>
    </div>

    <div class="modalLoading" id="modalLoading">
        <div class="modalLoading-content">
            <div class="loader"></div>
            <p>Aguarde, carregando...</p>
        </div>
    </div>

    <div class="Modal" id="ModalFaltantes">
        <div class="Modal-Content">
            <span class="Close-Modal" onclick="FecharModal('ModalFaltantes')">&times;</span>
            <h2 id="TituloModal"></h2>
            <div class="Dados">
                <ul id="ListaPeçasFaltantes"></ul>
            </div>
        </div>
    </div>


    <script>
        const ApiDistribuicaoMatriz = 'http://192.168.0.183:5000/api/FilaPedidos';
        const ApiDistribuicaoFilial = 'http://192.168.0.184:5000/api/FilaPedidos';
        const ApiUsuariosMatriz = "http://192.168.0.183:5000/api/Usuarios";
        const ApiUsuariosFilial = "http://192.168.0.184:5000/api/Usuarios";
        const ApiAtribuicaoMatriz = 'http://192.168.0.183:5000/api/AtribuirPedidos';
        const ApiAtribuicaoFilial = 'http://192.168.0.184:5000/api/AtribuirPedidos'
        const ApiPriorizaMatriz = "http://192.168.0.183:5000/api/Prioriza";
        const ApiPriorizaFilial = "http://192.168.0.184:5000/api/Prioriza";
        const ApiRecarregarPedidosMatriz = "http://192.168.0.183:5000/api/RecarregarPedidos?empresa=";
        const ApiRecarregarPedidosFilial = "http://192.168.0.184:5000/api/RecarregarPedidos?empresa=";
        const ApiIndicadorDistribuicaoMatriz = 'http://192.168.0.183:5000/api/IndicadorDistribuicao';
        const ApiIndicadorDistribuicaoFilial = 'http://192.168.0.184:5000/api/IndicadorDistribuicao';
        const ApiMatrizFalta = `http://192.168.0.183:5000/api/DetalharPedido?codPedido=`
        const ApiFilialFalta = `http://192.168.0.184:5000/api/DetalharPedido?codPedido=`
        var empresa = '<?php echo $empresa; ?>';
        var usuario = '<?php echo $usuario; ?>';
        const Token = 'a40016aabcx9';
        const TabelaPedidos = document.getElementById('TabelaDistribuicao');



        function mostrarModalLoading() {
            document.getElementById("modalLoading").style.display = "block";
        }


        function ocultarModalLoading() {
            document.getElementById("modalLoading").style.display = "none";
        }

        function AbrirModal(Modal) {
            document.getElementById(Modal).style.display = 'block';
        };

        function FecharModal(Modal) {
            document.getElementById(Modal).style.display = 'none';
        }

        function formatarDados(data) {
            return data.map(item => {
                return {
                    '1- nome': item['1- nome'],
                    '2- qtdPedidos': item['2- qtdPedidos'],
                    '3- qtdepçs': item['3- qtdepçs'],
                    '4- MedPecas': item['4- Méd. pç/pedido'],
                    '5- Valor Atribuido': item['5- Valor Atribuido']
                };
            });
        }
//----------------------------------------------FUNÇÕES DE CONSULTAS (METHOD GET)-------------------------------------------------------------------------------//    

        async function obterFilaPedidos(API) {
            mostrarModalLoading();
            try {
                const response = await fetch(API, {
                    method: 'GET',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': Token
                    },
                });

                if (response.ok) {
                    const data = await response.json();
                    const TipoNotaFiltrado = data.filter(item => item["03-TipoNota"] !== "39 - BN MPLUS");

                    TipoNotaFiltrado.forEach(item => {
                        item["18-%Reposto"] = parseFloat(item["18-%Reposto"]);
                        item["18-%Reposto"] = (item["18-%Reposto"] * 1).toFixed(2);
                        item["18-%Reposto"] += "%";
                        item["20-Separado%"] = parseFloat(item["20-Separado%"]);
                        item["20-Separado%"] = (item["20-Separado%"] * 1).toFixed(2);
                        item["20-Separado%"] += "%";
                        item["14-AgrupamentosPedido"] = item["14-AgrupamentosPedido"].replaceAll('/', ',');
                    });

                    CriarListaPedidos(TipoNotaFiltrado);
                    PintarPedidosCompletos();
                    marcarLinhasDuplicadas();
                    PintarPedidosUrgentes();
                    ocultarModalLoading();
                } else {
                    throw new Error('Erro no retorno');
                    ocultarModalLoading();
                }
            } catch (error) {
                console.error(error.message);
                ocultarModalLoading();
            }
        };


        async function CarregarUsuarios(API) {
            mostrarModalLoading();
            const selecaoUsuarios = document.getElementById('Usuarios');
            selecaoUsuarios.innerHTML = ''; // Limpa os valores antigos da combobox

            const opcaoInicial = document.createElement('option');
            opcaoInicial.textContent = 'Selecione um Separador para Atribuição!'; // Texto inicial
            opcaoInicial.disabled = true;
            opcaoInicial.selected = true;
            selecaoUsuarios.appendChild(opcaoInicial);
            try {
                const response = await fetch(API, {
                    method: 'GET',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': Token
                    },
                });

                if (response.ok) {
                    const data = await response.json();
                    data.forEach(item => {
                        const novoItem = document.createElement('option');
                        novoItem.textContent = `${item.codigo}-${item.nome}`;
                        selecaoUsuarios.appendChild(novoItem);
                    });
                    ocultarModalLoading();
                } else {
                    throw new Error('Erro no retorno');
                    ocultarModalLoading();
                }
            } catch (error) {
                console.error(error.message);
                ocultarModalLoading();
            }
        };

        async function RecarregarPedidos(API, Empresa) {
            mostrarModalLoading();
            try {
                const response = await fetch(`${API}${Empresa}`, {
                    method: 'GET',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': Token
                    },
                });

                if (response.ok) {
                    const data = await response.json();
                    if (empresa === "1") {
                        await obterFilaPedidos(ApiDistribuicaoMatriz)
                    } else if (empresa === "4") {
                        await obterFilaPedidos(ApiDistribuicaoFilial)
                    }
                    ocultarModalLoading();
                    Swal.fire({
                        title: "Atualização Concluida",
                        icon: "success",
                        showConfirmButton: false,
                        timer: 3000,
                    });
                } else {
                    throw new Error('Erro no retorno');
                    ocultarModalLoading();
                    Swal.fire({
                        title: "Erro",
                        icon: "error",
                        showConfirmButton: false,
                        timer: 3000,
                    });
                }
            } catch (error) {
                console.error(error.message);
                ocultarModalLoading();
                Swal.fire({
                        title: "Erro",
                        icon: "error",
                        showConfirmButton: false,
                        timer: 3000,
                    });
            }
        };

        async function CarregarInformacoes(API) {
            mostrarModalLoading();
            try {
                const response = await fetch(API, {
                    method: 'GET',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': Token
                    },
                });

                if (response.ok) {
                    const data = await response.json();
                    const dadosFormatados = formatarDados(data);
                    CriarListaInformacoes(dadosFormatados)
                    ocultarModalLoading();
                } else {
                    throw new Error('Erro no retorno');
                    ocultarModalLoading();
                }
            } catch (error) {
                console.error(error.message);
                ocultarModalLoading();
            }
        };

        async function PecasFaltantes(API) {
            const PedidosSelecionados = capturarItensSelecionados();
            mostrarModalLoading();
            try {
                const response = await fetch(`${API}${PedidosSelecionados}`, {
                    method: 'GET',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': Token
                    },
                });

                if (response.ok) {
                    const data = await response.json();
                    const detalhamentoPedido = data[0]["5- Detalhamento dos Sku:"];
                    const Filtro = detalhamentoPedido.filter(item => item["endereco"] === "Não Reposto");
    
                    const listaPeçasFaltantes = document.getElementById("ListaPeçasFaltantes");
                    listaPeçasFaltantes.innerHTML = ''; // Limpa o conteúdo atual
    
                    Filtro.forEach(item => {
                        const listItem = document.createElement("li");
                        listItem.textContent = `${item["referencia"]} / ${item["tamanho"]} / ${item["cor"]} - ${item["reduzido"]}`;
                        listaPeçasFaltantes.appendChild(listItem);
           
                    });
                    AbrirModal('ModalFaltantes');
                    document.getElementById('TituloModal').textContent = `Peças Faltantes no Pedido: ${PedidosSelecionados}`;
                    ocultarModalLoading();
                } else {
                    throw new Error('Erro no retorno');
                    ocultarModalLoading();
                    Swal.fire({
                        title: "Erro",
                        icon: "error",
                        showConfirmButton: false,
                        timer: 3000,
                    });
                }
            } catch (error) {
                console.error(error.message);
                ocultarModalLoading();
                Swal.fire({
                        title: "Erro",
                        icon: "error",
                        showConfirmButton: false,
                        timer: 3000,
                    });
            }
        };

//--------------------------------------------------------------------------------------------------------------------------------------------------------------------//

//-----------------------------------------------------------------FUNÇÕES PARA CRIAR AS TABELAS---------------------------------------------------------------------//
        function CriarListaPedidos(ListaPedidos) {
            $('#PaginacaoUsuarios .dataTables_paginate').remove();

            if ($.fn.DataTable.isDataTable('#TabelaDistribuicao')) {
                $('#TabelaDistribuicao').DataTable().destroy();
            }

            tabela = $('#TabelaDistribuicao').DataTable({
                paging: false,
                info: false,
                searching: true,
                colReorder: true,
                colResize: true,
                autoWidth: false, // Desativa a largura automática das colunas
                columns: [
                    {
                        data: null,
                        render: function (data, type, row, meta) {
                            // Cria um checkbox com ID único para cada linha
                            return '<input type="checkbox" class="rowCheckbox" id="checkbox_' + meta.row + '">';
                        }
                    },
                    { data: '01-CodPedido' },
                    { data: '11-NomeUsuarioAtribuido' },
                    { data: '03-TipoNota' },
                    { data: '02- Data Sugestao' },
                    { data: '15-qtdesugerida' },
                    { data: '18-%Reposto' },
                    { data: '20-Separado%' },
                    { data: '12-vlrsugestao' },
                    { data: '14-AgrupamentosPedido' },
                    { data: '07-estado' },
                    { data: '22- situacaopedido' },
                    { data: '21-MARCA' },
                    { data: 'prioridade' },
                    { data: '23-transportadora' },
                ],
                language: {
                    paginate: {
                        first: 'Primeira',
                        previous: 'Anterior',
                        next: 'Próxima',
                        last: 'Última',
                    },
                }
            });

            tabela.clear().rows.add(ListaPedidos).draw();

            $('.dataTables_paginate').appendTo('#PaginacaoUsuarios');

            $('#PaginacaoUsuarios .paginate_button.previous').on('click', function () {
                tabela.page('previous').draw('page');
            });

            $('#PaginacaoUsuarios .paginate_button.next').on('click', function () {
                tabela.page('next').draw('page');
            });

            const paginaInicial = 1;
            tabela.page(paginaInicial - 1).draw('page');

            $('#PaginacaoUsuarios .paginate_button').on('click', function () {
                $('#PaginacaoUsuarios .paginate_button').removeClass('current');
                $(this).addClass('current');
            });
        };

        function CriarListaInformacoes(ListaInformacoes) {
            $('#PaginacaoUsuarios .dataTables_paginate').remove();

            if ($.fn.DataTable.isDataTable('#TabelaMediaPecas')) {
                $('#TabelaMediaPecas').DataTable().destroy();
            }

            tabela = $('#TabelaMediaPecas').DataTable({
                paging: false,
                info: false,
                searching: true,
                colReorder: true,
                colResize: true,
                autoWidth: false, // Desativa a largura automática das colunas
                columns: [
    { data: '1- nome' },
    { data: '2- qtdPedidos' },
    { data: '3- qtdepçs' },
    { data: '4- MedPecas' }, // Atualização aqui
    { data: '5- Valor Atribuido' },
],
                language: {
                    paginate: {
                        first: 'Primeira',
                        previous: 'Anterior',
                        next: 'Próxima',
                        last: 'Última',
                    },
                }
            });

            tabela.clear().rows.add(ListaInformacoes).draw();

            $('.dataTables_paginate').appendTo('#PaginacaoUsuarios');

            $('#PaginacaoUsuarios .paginate_button.previous').on('click', function () {
                tabela.page('previous').draw('page');
            });

            $('#PaginacaoUsuarios .paginate_button.next').on('click', function () {
                tabela.page('next').draw('page');
            });

            const paginaInicial = 1;
            tabela.page(paginaInicial - 1).draw('page');

            $('#PaginacaoUsuarios .paginate_button').on('click', function () {
                $('#PaginacaoUsuarios .paginate_button').removeClass('current');
                $(this).addClass('current');
            });
        };

//--------------------------------------------------------------------------------------------------------------------------------------------------------------------//

//-------------------------------------------------------FUNÇÃO PARA CAPTURAR AS CHECKBOX SELECIONADAS---------------------------------------------------------------//

        function capturarItensSelecionados() {
            const linhasTabela = document.getElementById('TabelaDistribuicao').getElementsByTagName('tr');
            const PedidosSelecionados = [];

            for (let i = 1; i < linhasTabela.length; i++) {
                const linha = linhasTabela[i];
                const checkbox = linha.querySelector('input[type="checkbox"]');

                if (checkbox.checked) {
                    const colunas = linha.getElementsByTagName('td');
                    const Pedidos = colunas[9].textContent.trim(); // Substitua o índice (9) pela coluna desejada

                    // Verifica se há mais de um pedido na string e separa-os em uma lista
                    const pedidosSeparados = Pedidos.split(',').map(pedido => pedido.trim());

                    // Adiciona os pedidos à lista
                    PedidosSelecionados.push(...pedidosSeparados);
                }
            }

            return PedidosSelecionados;
        };

//--------------------------------------------------------------------------------------------------------------------------------------------------------------------//

//-------------------------------------------------------------------FUNÇÃO DE ATRIBUIR PEDIDOS----------------------------------------------------------------------//

        async function AtribuicaoPedidos(Api) {
            mostrarModalLoading();
            const PedidosSelecionados = capturarItensSelecionados();

            try {
                if (PedidosSelecionados.length === 0) {
                    Swal.fire({
                        title: "Nenhum Pedido Selecionado",
                        icon: "error",
                        showConfirmButton: false,
                        timer: 3000,
                    });
                    if (empresa === "1") {
                        await CarregarUsuarios(ApiUsuariosMatriz);
                    } else if (empresa === "4") {
                        await CarregarUsuarios(ApiUsuariosFilial);
                    }
                } else {
                    const comboboxUsuarios = document.getElementById('Usuarios');
                    const usuarioSelecionado = comboboxUsuarios.value;
                    const codigoUsuario = usuarioSelecionado.split('-')[0].trim();

                    let Atribuicao = {
                        codUsuario: codigoUsuario,
                        pedidos: PedidosSelecionados,
                        data: "2023-06-22 08:00:00"
                    };

                    const response = await fetch(Api, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': Token
                        },
                        body: JSON.stringify(Atribuicao),
                    });

                    if (response.ok) {
                        const data = await response.json();
                        if (empresa === "1") {
                            await obterFilaPedidos(ApiDistribuicaoMatriz);
                            await CarregarUsuarios(ApiUsuariosMatriz);
                            //PassarInformacoes(ApiIndicadorDistribuicaoMatriz);

                            console.log(data);
                        } else if (empresa === "4") {
                            await obterFilaPedidos(ApiDistribuicaoFilial);
                            await CarregarUsuarios(ApiUsuariosMatriz);
                            //PassarInformacoes(ApiIndicadorDistribuicaoFilial);
                        }
                        Swal.fire({
                            title: "Pedido(s) Atribuído(s)",
                            icon: "success",
                            showConfirmButton: false,
                            timer: 3000,
                        });
                    } else {
                        throw new Error('Erro ao obter os dados da API');
                    }
                }
            } catch (error) {
                console.error(error);
                Swal.fire({
                    title: "Erro",
                    icon: "error",
                    showConfirmButton: false,
                    timer: 3000,
                });
                if (empresa === "1") {
                    await obterFilaPedidos(ApiDistribuicaoMatriz);
                    //PassarInformacoes(ApiIndicadorDistribuicaoMatriz)
                    console.log(data);
                } else if (empresa === "4") {
                    await obterFilaPedidos(ApiDistribuicaoFilial);
                    //PassarInformacoes(ApiIndicadorDistribuicaoFilial)
                }
            }
        }

//---------------------------------------------------------------------------------------------------------------------------------------------------------------------//


//------------------------------------------------------------------------FUNÇÃO PARA DEFINIR AS PRIORIDADES-----------------------------------------------------------//

async function DefinirPrioridade(Api) {
    mostrarModalLoading();
    const PedidosSelecionados = capturarItensSelecionados();

    if (PedidosSelecionados.length === 0) {
        Swal.fire({
            title: "Nenhum Pedido Selecionado",
            icon: "error",
            showConfirmButton: false,
            timer: 3000,
        });
        if (empresa === "1") {
            await CarregarUsuarios(ApiUsuariosMatriz);
        } else if (empresa === "4") {
            await CarregarUsuarios(ApiUsuariosFilial);
        }
    } else {
        let Atribuicao = {
            pedidos: PedidosSelecionados,
        };

        try {
            const response = await fetch(Api, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': Token
                },
                body: JSON.stringify(Atribuicao),
            });

            if (response.ok) {
                const data = await response.json();
                if (empresa === "1") {
                            await obterFilaPedidos(ApiDistribuicaoMatriz);
                        } else if (empresa === "4") {
                            await obterFilaPedidos(ApiDistribuicaoFilial);    
                        }
                Swal.fire({
                    title: "Prioridade Alterada",
                    icon: "success",
                    showConfirmButton: false,
                    timer: 3000,
                });
                
            } else {
                throw new Error('Erro ao obter os dados da API');
                if (empresa === "1") {
                            await obterFilaPedidos(ApiDistribuicaoMatriz);
                        } else if (empresa === "4") {
                            await obterFilaPedidos(ApiDistribuicaoFilial);    
                        }
                Swal.fire({
                    title: "Erro",
                    icon: "error",
                    showConfirmButton: false,
                    timer: 3000,
                });
            }
        } catch (error) {
            console.error(error);
            if (empresa === "1") {
                            await obterFilaPedidos(ApiDistribuicaoMatriz);
                        } else if (empresa === "4") {
                            await obterFilaPedidos(ApiDistribuicaoFilial);    
                        }
                Swal.fire({
                    title: "Erro",
                    icon: "error",
                    showConfirmButton: false,
                    timer: 3000,
                });
        }
    }
}


//--------------------------------------------------------------------------------------------------------------------------------------------------------------------//

//------------------------------------------------------------------------FUNCOES DE COLORAÇÃO DAS LINHAS-------------------------------------------------------------//

//-------------------------------------------------------------------------------------------------------------------------------------------------------------------//

function PintarPedidosCompletos() {
        const colunaDesejada = 6; // Índice da coluna com base em 0 (coluna 7 na contagem padrão)
        const linhasTabela = TabelaPedidos.getElementsByTagName('tr');
    
        // Pinta a coluna de verde se o valor for igual a "100.00"
        for (let i = 1; i < linhasTabela.length; i++) {
            const linha = linhasTabela[i];
            const colunaValor = linha.querySelector(`td:nth-child(${colunaDesejada + 1})`);
            const valor = colunaValor.textContent.trim();
    
            if (valor === "100.00%") {
                colunaValor.style.backgroundColor = 'lightgreen'; // Pinta a célula da coluna de verde
            } else {
                colunaValor.style.backgroundColor = ''; // Remove a cor de fundo da célula caso não seja igual a "100.00"
            }
        }
    }

    function PintarPedidosUrgentes() {
        const colunaDesejada = 13; // Índice da coluna com base em 0 (coluna 7 na contagem padrão)
        const linhasTabela = TabelaPedidos.getElementsByTagName('tr');
    
        // Pinta a coluna de verde se o valor for igual a "100.00"
        for (let i = 1; i < linhasTabela.length; i++) {
            const linha = linhasTabela[i];
            const colunaValor = linha.querySelector(`td:nth-child(${colunaDesejada + 1})`);
            const valor = colunaValor.textContent.trim();
    
            if (valor === "URGENTE") {
                colunaValor.style.backgroundColor = 'Red'; // Pinta a célula da coluna de verde
            } else {
                colunaValor.style.backgroundColor = ''; // Remove a cor de fundo da célula caso não seja igual a "100.00"
            }
        }
    }
    


function marcarLinhasDuplicadas() {
    const valoresContados = {};
    const colunaDesejada = 9; // Índice da coluna "Usuário Atribuído" na tabela (lembrando que a contagem começa em 0)
        
    const linhasTabela = TabelaPedidos.getElementsByTagName('tr');
        
            // Conta quantas vezes cada valor aparece na coluna desejada
    for (let i = 1; i < linhasTabela.length; i++) {
        const linha = linhasTabela[i];
        const colunaValor = linha.querySelector(`td:nth-child(${colunaDesejada + 1})`);
        const valor = colunaValor.textContent.trim();
        
        if (valor in valoresContados) {
            valoresContados[valor]++;
            } else {
                valoresContados[valor] = 1;
            }
        }
        
// Pinta todas as linhas que possuem valores duplicados
    for (let i = 1; i < linhasTabela.length; i++) {
        const linha = linhasTabela[i];
        const colunaValor = linha.querySelector(`td:nth-child(${colunaDesejada + 1})`);
        const valor = colunaValor.textContent.trim();
        
        if (valoresContados[valor] > 1) {
            linha.style.backgroundColor = 'yellow'; // Altera a cor de fundo da linha para amarelo (indicando duplicata)
        } else {
            linha.style.backgroundColor = ''; // Remove a cor de fundo caso não seja duplicata
        }
        }
}

//------------------------------------------------------------------------DEFINIÇÃO DE LISTENNERS----------------------------------------------------------------------//

        


        document.getElementById('Usuarios').addEventListener('change', () => {
            console.log('Capturando itens selecionados...');
            if (empresa === "1") {
                AtribuicaoPedidos(ApiAtribuicaoMatriz);
            } else if (empresa === "4") {
                AtribuicaoPedidos(ApiAtribuicaoFilial);
            }
        });

        document.getElementById('Priorizacao').addEventListener('click', () =>{
            if (empresa === "1") {
                DefinirPrioridade(ApiPriorizaMatriz);
            } else if (empresa === "4") {
                DefinirPrioridade(ApiPriorizaFilial);
            }
        });

        document.getElementById('Atualizar').addEventListener('click', async() => {
            if (empresa === "1") {
                await RecarregarPedidos(ApiRecarregarPedidosMatriz, empresa);
            } else if (empresa === "4") {
                await RecarregarPedidos(ApiRecarregarPedidosFilial, empresa);
            } else {
            }
   
        });

        document.getElementById('VerificarPecasFaltando').addEventListener('click', async() => {
            if (empresa === "1") {
                await PecasFaltantes(ApiMatrizFalta);
            } else if (empresa === "4") {
                await PecasFaltantes(ApiFilialFalta);
            } else {
            }
   
        });

        

        document.getElementById('Informacoes').addEventListener('click', async() => {
            const TabelaDistribuicao =  document.getElementById('TabelaDistribuicao');
            const TabelaMediaPecas = document.getElementById('TabelaMediaPecas');
            const DivTabelaMediaPecas = document.getElementById('DivTabelaMediaPecas');
            const DivTabelaDistribuicao = document.getElementById('DivTabelaDistribuicao');
            const BotaoVoltar = document.getElementById('Voltar');
            const BotaoInformacoes = document.getElementById('Informacoes');
            if (empresa === "1") {
                await CarregarInformacoes(ApiIndicadorDistribuicaoMatriz);
                DivTabelaDistribuicao.style.display = 'none';
                DivTabelaMediaPecas.style.display = 'block';
                BotaoVoltar.style.display = 'block';
                BotaoInformacoes.style.display = 'none';
            } else if (empresa === "4") {
                await CarregarInformacoes(ApiIndicadorDistribuicaoFilial);
                DivTabelaMediaPecas.style.display = 'block';
                DivTabelaDistribuicao.style.display = 'none';
                BotaoVoltar.style.display = 'block';
                BotaoInformacoes.style.display = 'none';
            } else {
            }
   
        });

        document.getElementById('Voltar').addEventListener('click', async() => {
            const TabelaDistribuicao =  document.getElementById('TabelaDistribuicao');
            const TabelaMediaPecas = document.getElementById('TabelaMediaPecas');
            const DivTabelaMediaPecas = document.getElementById('DivTabelaMediaPecas');
            const DivTabelaDistribuicao = document.getElementById('DivTabelaDistribuicao');
            const BotaoVoltar = document.getElementById('Voltar');
            const BotaoInformacoes = document.getElementById('Informacoes');

            if (empresa === "1") {
                await obterFilaPedidos(ApiDistribuicaoMatriz);
                DivTabelaDistribuicao.style.display = 'block';
                DivTabelaMediaPecas.style.display = 'none';
                BotaoVoltar.style.display = 'none';
                BotaoInformacoes.style.display = 'block';
                
            } else if (empresa === "4") {
                await obterFilaPedidos(ApiDistribuicaoFilial);
                DivTabelaMediaPecas.style.display = 'none';
                DivTabelaDistribuicao.style.display = 'block';
                BotaoVoltar.style.display = 'none';
                BotaoInformacoes.style.display = 'block';
            } else {
            }
   
        });
        

        window.addEventListener('load', async () => {
            if (empresa === "1") {
                await obterFilaPedidos(ApiDistribuicaoMatriz);
                CarregarUsuarios(ApiUsuariosMatriz);

            } else if (empresa === "4") {
                await obterFilaPedidos(ApiDistribuicaoFilial);
                CarregarUsuarios(ApiUsuariosFilial);
            }
        });  






        
    </script>
</body>

</html>