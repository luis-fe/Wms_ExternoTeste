const Empresa = localStorage.getItem('CodEmpresa');

const rootElement = document.documentElement;

if (Empresa === "1") {
    rootElement.classList.add('palheta-empresa-a');
} else if (Empresa === "4") {
    rootElement.classList.add('palheta-empresa-b');
} else {
    window.location.href = '/Login_Teste';
}

const ApiDistribuicaoMatriz = 'http://192.168.0.183:5000/api/FilaPedidos';
const ApiDistribuicaoFilial = 'http://192.168.0.184:5000/api/FilaPedidos';
const indiceExibicao = 0;
let PedidosSelecionados = [];
let UsuarioSelecionado;
let PedidosExibicao = [];
let ordenacaoAscendente = true;



function criarTabelaDistribuicao(listaPedidos) {
    const tabela = document.getElementById('TabelaTabelaDistribuicao');
    tabela.innerHTML = ''; // Limpa o conteúdo da tabela antes de preenchê-la novamente

    // Cria o cabeçalho da tabela
    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const colunaCheckbox = document.createElement('th');
    const ColunaPedido = document.createElement('th');
    const botaoProduto = document.createElement('button');
    botaoProduto.id = 'ButtonProduto';
    botaoProduto.classList.add('botaoProduto');
    const imagemBotao = document.createElement('img');
    imagemBotao.src = 'static/IMAGENS/IconeOrdenacao.png';
    botaoProduto.appendChild(imagemBotao);
    botaoProduto.addEventListener('click', () => {
    if (Empresa === "1") {
        OrdenarTabela("01-CodPedido", ApiOrdenacaoMatriz);
    } else if (Empresa === "4") {
        OrdenarTabela("01-CodPedido", ApiOrdenacaoFilial);
    }
});



    const ColunaNomeUsuario = document.createElement('th');
    const botaoUsuario = document.createElement('button');
    botaoUsuario.id = 'ButtonUsuario';
    botaoUsuario.classList.add('botaoUsuario');
    const imagemBotao1 = document.createElement('img');
    imagemBotao1.src = 'static/IMAGENS/IconeOrdenacao.png';
    botaoUsuario.appendChild(imagemBotao1);
    botaoUsuario.addEventListener('click', () => { if (Empresa === "1") {
        OrdenarTabela("11-NomeUsuarioAtribuido", ApiOrdenacaoMatriz);
    } else if (Empresa === "4") {
        OrdenarTabela("11-NomeUsuarioAtribuido", ApiOrdenacaoFilial);
    }});

    const ColunaTipoDeNota = document.createElement('th');
    const botaoNota = document.createElement('button');
    botaoNota.id = 'ButtonNota';
    botaoNota.classList.add('botaoNota');
    const imagemBotao2 = document.createElement('img');
    imagemBotao2.src = 'static/IMAGENS/IconeOrdenacao.png';
    botaoNota.appendChild(imagemBotao2);
    botaoNota.addEventListener('click', () => { if (Empresa === "1") {
        OrdenarTabela("03-TipoNota", ApiOrdenacaoMatriz);
    } else if (Empresa === "4") {
        OrdenarTabela("03-TipoNota", ApiOrdenacaoFilial);
    }});


    const ColunaDataSugestao = document.createElement('th');
    const botaoData = document.createElement('button');
    botaoData.id = 'ButtonData';
    botaoData.classList.add('botaoData');
    const imagemBotao3 = document.createElement('img');
    imagemBotao3.src = 'static/IMAGENS/IconeOrdenacao.png';
    botaoData.appendChild(imagemBotao3);
    botaoData.addEventListener('click', () => { if (Empresa === "1") {
        OrdenarTabela("02- Data Sugestao", ApiOrdenacaoMatriz);
    } else if (Empresa === "4") {
        OrdenarTabela("02- Data Sugestao", ApiOrdenacaoFilial);
    }});

    const ColunaQuantidadePeças = document.createElement('th');
    const botaoQuantidade = document.createElement('button');
    botaoQuantidade.id = 'ButtonQuantidade';
    botaoQuantidade.classList.add('botaoQuantidade');
    const imagemBotao4 = document.createElement('img');
    imagemBotao4.src = 'static/IMAGENS/IconeOrdenacao.png';
    botaoQuantidade.appendChild(imagemBotao4);
    botaoQuantidade.addEventListener('click', () => { if (Empresa === "1") {
        OrdenarTabela("15-qtdesugerida", ApiOrdenacaoMatriz);
    } else if (Empresa === "4") {
        OrdenarTabela("15-qtdesugerida", ApiOrdenacaoFilial);
    }});
    

    const ColunaTotalReposto = document.createElement('th');
    const botaoReposto = document.createElement('button');
    botaoReposto.id = 'ButtonReposto';
    botaoReposto.classList.add('botaoQuantidade');
    const imagemBotao5 = document.createElement('img');
    imagemBotao5.src = 'static/IMAGENS/IconeOrdenacao.png';
    botaoReposto.appendChild(imagemBotao5);
    botaoReposto.addEventListener('click', () => { if (Empresa === "1") {
        OrdenarTabela("18-%Reposto", ApiOrdenacaoMatriz);
    } else if (Empresa === "4") {
        OrdenarTabela("18-%Reposto", ApiOrdenacaoFilial);
    }});


    const ColunaTotalSeparado = document.createElement('th');
    const botaoSeparado = document.createElement('button');
    botaoSeparado.id = 'ButtonSeparado';
    botaoSeparado.classList.add('botaoQuantidade');
    const imagemBotao6 = document.createElement('img');
    imagemBotao6.src = 'static/IMAGENS/IconeOrdenacao.png';
    botaoSeparado.appendChild(imagemBotao6);
    botaoSeparado.addEventListener('click', () => { if (Empresa === "1") {
        OrdenarTabela("20-Separado%", ApiOrdenacaoMatriz);
    } else if (Empresa === "4") {
        OrdenarTabela("20-Separado%", ApiOrdenacaoFilial);
    }});


    const ColunaValorPedido = document.createElement('th');
    const botaoValor = document.createElement('button');
    botaoValor.id = 'ButtonValor';
    botaoValor.classList.add('botaoQuantidade');
    const imagemBotao7 = document.createElement('img');
    imagemBotao7.src = 'static/IMAGENS/IconeOrdenacao.png';
    botaoValor.appendChild(imagemBotao7);
    botaoValor.addEventListener('click', () => { if (Empresa === "1") {
        OrdenarTabela("12-vlrsugestao", ApiOrdenacaoMatriz);
    } else if (Empresa === "4") {
        OrdenarTabela("12-vlrsugestao", ApiOrdenacaoFilial);
    }});

    const ColunaPedidosAgrupados = document.createElement('th');
    const ColunaEstado = document.createElement('th');
    const ColunaSituacaoPedido = document.createElement('th');
    const ColunaMarca = document.createElement('th');
    const ColunaPrioridade = document.createElement('th');
    const ColunaTransportadora = document.createElement('th');

    colunaCheckbox.textContent = '';
    ColunaPedido.textContent = 'Pedido';

    ColunaNomeUsuario.textContent = 'Usuário Atribuído';
    ColunaTipoDeNota.textContent = 'Tipo de Nota';
    ColunaDataSugestao.textContent = 'Data Sugestão';
    ColunaQuantidadePeças.textContent = 'Quantidade Peças';
    ColunaTotalReposto.textContent = '% Reposto';
    ColunaTotalSeparado.textContent = '% Separado';
    ColunaValorPedido.textContent = 'Valor R$';
    ColunaPedidosAgrupados.textContent = 'Pedidos Agrupados';
    ColunaEstado.textContent = 'Estado';
    ColunaSituacaoPedido.textContent = 'Situacao Pedido';
    ColunaMarca.textContent = 'Marca';
    ColunaPrioridade.textContent = 'Prioridade';
    ColunaTransportadora.textContent = 'Transportadora';
    
    colunaCheckbox.style.width = '30px';
    ColunaPedido.style.width = '100px'
    ColunaNomeUsuario.style.width = '250px'
    ColunaTipoDeNota.style.width = '150px'
    ColunaDataSugestao.style.width = '100px'
    ColunaQuantidadePeças.style.width = '100px'
    ColunaTotalReposto.style.width = '100px'
    ColunaTotalSeparado.style.width = '100px'
    ColunaValorPedido.style.width = '150px'
    ColunaPedidosAgrupados.style.width = '150px'
    ColunaEstado.style.width = '100px'
    ColunaSituacaoPedido.style.width = '100px'
    ColunaMarca.style.width = '100px'
    ColunaTransportadora.style.width = '250px'

    ColunaPedido.appendChild(botaoProduto);
    ColunaNomeUsuario.appendChild(botaoUsuario);
    ColunaTipoDeNota.appendChild(botaoNota);
    ColunaDataSugestao.appendChild(botaoData);
    ColunaQuantidadePeças.appendChild(botaoQuantidade);
    ColunaTotalReposto.appendChild(botaoReposto);
    ColunaTotalSeparado.appendChild(botaoSeparado);
    ColunaValorPedido.appendChild(botaoValor);
    cabecalhoRow.appendChild(colunaCheckbox);
    cabecalhoRow.appendChild(ColunaPedido);
    cabecalhoRow.appendChild(ColunaNomeUsuario);
    cabecalhoRow.appendChild(ColunaTipoDeNota);
    cabecalhoRow.appendChild(ColunaDataSugestao);
    cabecalhoRow.appendChild(ColunaQuantidadePeças);
    cabecalhoRow.appendChild(ColunaTotalReposto);
    cabecalhoRow.appendChild(ColunaTotalSeparado);
    cabecalhoRow.appendChild(ColunaValorPedido);
    cabecalhoRow.appendChild(ColunaMarca);
    cabecalhoRow.appendChild(ColunaPrioridade);
    cabecalhoRow.appendChild(ColunaPedidosAgrupados);
    cabecalhoRow.appendChild(ColunaEstado);
    cabecalhoRow.appendChild(ColunaSituacaoPedido);
    cabecalhoRow.appendChild(ColunaTransportadora)
    cabecalho.appendChild(cabecalhoRow);
    tabela.appendChild(cabecalho);

    // Preenche a tabela com os dados dos usuários
    PedidosExibicao = listaPedidos.slice(indiceExibicao, indiceExibicao + 99999);

    PedidosExibicao.forEach(item => {
        const row = document.createElement('tr');
        const colunaCheckbox = document.createElement('td');
        const ColunaPedido = document.createElement('td');
        const ColunaNomeUsuario = document.createElement('td');
        const ColunaTipoDeNota = document.createElement('td');
        const ColunaDataSugestao = document.createElement('td');
        const ColunaQuantidadePeças = document.createElement('td');
        const ColunaTotalReposto = document.createElement('td');
        const ColunaTotalSeparado = document.createElement('td');
        const ColunaValorPedido = document.createElement('td');
        const ColunaPedidosAgrupados = document.createElement('td');
        const ColunaEstado = document.createElement('td');
        const ColunaSituacaoPedido = document.createElement('td');
        const ColunaMarca = document.createElement('td');
        const ColunaTransportadora = document.createElement('td');
        const ColunaPrioridade = document.createElement('td');

        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox'; // Alterado de 'radio' para 'checkbox'
        checkbox.value = item["01-CodPedido"];
        checkbox.name = 'checkboxPedido';
        colunaCheckbox.appendChild(checkbox);
        ColunaPedido.textContent = item["01-CodPedido"];
        ColunaNomeUsuario.textContent = item["11-NomeUsuarioAtribuido"];
        ColunaTipoDeNota.textContent = item["03-TipoNota"];
        ColunaDataSugestao.textContent = new Date(item["02- Data Sugestao"]).toLocaleDateString('pt-BR')
        ColunaQuantidadePeças.textContent = item["15-qtdesugerida"];
        ColunaTotalReposto.textContent = item["18-%Reposto"];
        ColunaTotalSeparado.textContent = item["20-Separado%"];
        ColunaValorPedido.textContent = item["12-vlrsugestao"];
        ColunaPedidosAgrupados.textContent = item["14-AgrupamentosPedido"];
        ColunaEstado.textContent = item["07-estado"];
        ColunaSituacaoPedido.textContent = item["22- situacaopedido"];
        ColunaMarca.textContent = item["21-MARCA"];
        ColunaTransportadora.textContent = item["23-transportadora"];
        ColunaPrioridade.textContent = item["prioridade"];
        row.appendChild(colunaCheckbox);
        row.appendChild(ColunaPedido);
        row.appendChild(ColunaNomeUsuario);
        row.appendChild(ColunaTipoDeNota);
        row.appendChild(ColunaDataSugestao);
        row.appendChild(ColunaQuantidadePeças);
        row.appendChild(ColunaTotalReposto);
        row.appendChild(ColunaTotalSeparado);
        row.appendChild(ColunaValorPedido);
        row.appendChild(ColunaMarca);
        row.appendChild(ColunaPrioridade);
        row.appendChild(ColunaPedidosAgrupados);
        row.appendChild(ColunaEstado);
        row.appendChild(ColunaSituacaoPedido);
        row.appendChild(ColunaTransportadora);
        tabela.appendChild(row);
        });

    }

const modalLoading = document.getElementById("ModalLoading");

function AbrirModalLoading() {
    modalLoading.style.display = "block";
}

function FecharModalLoading() {
    modalLoading.style.display = "none";
}


async function CarregarDados(api) {
    AbrirModalLoading();

    try {
        const response = await fetch(api, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'a40016aabcx9'
            },
        });

        if (response.ok) {
            const data = await response.json();
            console.log(data);

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

            FecharModalLoading();
            criarTabelaDistribuicao(TipoNotaFiltrado);
            PintarPedidosCompletos();
            marcarLinhasDuplicadas();
            PintarPedidosUrgentes();
            if (Empresa === "1") {
                CarregarUsuarios(ApiUsuariosMatriz);
                PassarInformacoes(ApiIndicadorDistribuicaoMatriz);
            } else if (Empresa === "4") {
                CarregarUsuarios(ApiUsuariosFilial);
                PassarInformacoes(ApiIndicadorDistribuicaoMatriz);
            }
            
            
            
        } else {
            throw new Error('Erro ao obter a lista de usuários');
        }
    } catch (error) {
        console.error(error);
        FecharModalLoading();
    }
}



        
const ApiUsuariosMatriz = "http://192.168.0.183:5000/api/Usuarios";
const ApiUsuariosFilial = "http://192.168.0.184:5000/api/Usuarios"

function CarregarUsuarios(api) {
    const selecaoUsuarios = document.getElementById('Usuarios');
    selecaoUsuarios.innerHTML = ''; // Limpa os valores antigos da combobox
        
    const opcaoInicial = document.createElement('option');
    opcaoInicial.textContent = 'Selecione um Separador para Atribuição!'; // Texto inicial
    opcaoInicial.disabled = true;
    opcaoInicial.selected = true;
    selecaoUsuarios.appendChild(opcaoInicial);
        
    fetch(api, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'a40016aabcx9'
        },
     })
    .then(response => {
        if (response.ok) {
            return response.json();
        } else {
            throw new Error('Erro ao obter a lista de usuários');
        }
    })
    .then(data => {
        data.forEach(item => {

            const novoItem = document.createElement('option');
            novoItem.textContent = `${item.codigo}-${item.nome}`;
            selecaoUsuarios.appendChild(novoItem);

        });
    })
    .catch(error => {
        console.error(error);
            FecharModalLoading();
    });
}
        

window.addEventListener('load', async () => {
    const VerificaLogin = localStorage.getItem('Login');
    if (Empresa === "1") {
        if (VerificaLogin !== "Logado") {

            window.location.href = '/Login_Teste';
        } else {
            await AtualizarDados(ApiRecarregarPedidosMatriz, '1');
            CarregarDados(ApiDistribuicaoMatriz);
        }
    } else if (Empresa === "4") {
        if (VerificaLogin !== "Logado") {
            window.location.href = '/Login_Teste';
        } else {
            await AtualizarDados(ApiRecarregarPedidosFilial, '1');
            CarregarDados(ApiDistribuicaoFilial);
        }
    }
});
const InputBusca = document.getElementById('InputBusca');
const TabelaPedidos = document.getElementById('TabelaTabelaDistribuicao');
        
InputBusca.addEventListener('keyup', () => {
    const expressao = InputBusca.value.trim().toLowerCase();
    const linhasTabela = TabelaPedidos.getElementsByTagName('tr');
        
        for (let i = 1; i < linhasTabela.length; i++) {
            const linha = linhasTabela[i];
            const colunas = linha.getElementsByTagName('td');
            let encontrou = false;
        
        for (let j = 1; j < colunas.length; j++) {
            const conteudoColuna = colunas[j].textContent.trim().toLowerCase();
        
        if (conteudoColuna.includes(expressao)) {
            encontrou = true;
            break;
            }
        }
        
        if (encontrou) {
            linha.style.display = '';
        } else {
            linha.style.display = 'none';
        }
            PintarPedidosCompletos();
            marcarLinhasDuplicadas();
            PintarPedidosUrgentes();
        }
    });


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
        const colunaDesejada = 10; // Índice da coluna com base em 0 (coluna 7 na contagem padrão)
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
    const colunaDesejada = 11; // Índice da coluna "Usuário Atribuído" na tabela (lembrando que a contagem começa em 0)
        
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

function capturarItensSelecionados() {
    const linhasTabela = document.getElementById('TabelaTabelaDistribuicao').getElementsByTagName('tr');
    const PedidosSelecionados = [];

    for (let i = 1; i < linhasTabela.length; i++) {
        const linha = linhasTabela[i];
        const checkbox = linha.querySelector('input[type="checkbox"]');
        
        if (checkbox.checked) {
            const colunas = linha.getElementsByTagName('td');
            const Pedidos = colunas[11].textContent.trim(); // Substitua o índice (9) pela coluna desejada
            
            // Verifica se há mais de um pedido na string e separa-os em uma lista
            const pedidosSeparados = Pedidos.split(',').map(pedido => pedido.trim());
            
            // Adiciona os pedidos à lista
            PedidosSelecionados.push(...pedidosSeparados);
        }
    }

    return PedidosSelecionados;
}

const ApiAtribuicaoMatriz = 'http://192.168.0.183:5000/api/AtribuirPedidos';
const ApiAtribuicaoFilial = 'http://192.168.0.184:5000/api/AtribuirPedidos'

function AtribuicaoPedidos(api) {
    const PedidosSelecionados = capturarItensSelecionados();

    if (PedidosSelecionados.length === 0) {
        alert('Nenhum pedido selecionado');
        if (Empresa === "1") {
            CarregarUsuarios(ApiUsuariosMatriz);
        } else if (Empresa === "4") {
            CarregarUsuarios(ApiUsuariosFilial);
        }
    } else {
    const comboboxUsuarios1 = document.getElementById('Usuarios');
    const usuarioSelecionado = comboboxUsuarios1.value;
    const codigoUsuario = usuarioSelecionado.split('-')[0].trim();

    let Atribuicao = {
        codUsuario: codigoUsuario,
        pedidos: PedidosSelecionados,
        data: "2023-06-22 08:00:00"
    };

    fetch(api, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'a40016aabcx9'
        },
        body: JSON.stringify(Atribuicao),
    })
    .then(response => {
        if (response.ok) {
            return response.json();
        } else {
            throw new Error('Erro ao atribuir pedidos');
        }
    })
    .then(data => {
        if (Empresa === "1") {
            CarregarDados(ApiDistribuicaoMatriz);
            PassarInformacoes(ApiIndicadorDistribuicaoMatriz)
            console.log(data);
        } else if (Empresa === "4") {
            CarregarDados(ApiDistribuicaoFilial);
            PassarInformacoes(ApiIndicadorDistribuicaoFilial)
            console.log(data);
        }
       
        
    })
    .catch(error => {
        console.error(error);
    });
    
}}

const ApiPriorizaMatriz = "http://192.168.0.183:5000/api/Prioriza";
const ApiPriorizaFilial = "http://192.168.0.184:5000/api/Prioriza"

function DefinirPrioridade(api) {
    const PedidosSelecionados = capturarItensSelecionados();

    if (PedidosSelecionados.length === 0) {
        alert('Nenhum pedido selecionado');
        if (Empresa === "1") {
            CarregarUsuarios(ApiUsuariosMatriz);
        } else if (Empresa === "4") {
            CarregarUsuarios(ApiUsuariosFilial);
        }
    } else {

    let Atribuicao = {
        pedidos: PedidosSelecionados,
    };

    fetch(api, {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'a40016aabcx9'
        },
        body: JSON.stringify(Atribuicao),
    })
    .then(response => {
        if (response.ok) {
            return response.json();
        } else {
            throw new Error('Erro ao atribuir pedidos');
        }
    })
    .then(data => {
        console.log(data);
        
    })
    .catch(error => {
        console.error(error);
    });
    }}

    document.getElementById('Priorizacao').addEventListener('click', () =>{
        if (Empresa === "1") {
            DefinirPrioridade(ApiPriorizaMatriz);
        } else if (Empresa === "4") {
            DefinirPrioridade(ApiPriorizaFilial);
        }
    })
    

const comboboxUsuarios = document.getElementById('Usuarios');

comboboxUsuarios.addEventListener('change', () => {
    console.log('Capturando itens selecionados...');
    if (Empresa === "1") {
        AtribuicaoPedidos(ApiAtribuicaoMatriz);
    } else if (Empresa === "4") {
        AtribuicaoPedidos(ApiAtribuicaoFilial);
    }
});

ApiOrdenacaoMatriz = "http://192.168.0.183:5000/api/FilaPedidosClassificacao";
ApiOrdenacaoFilial = "http://192.168.0.184:5000/api/FilaPedidosClassificacao";


function OrdenarTabela(Coluna, api) {


    fetch(`${api}?coluna=${Coluna}&tipo=${ordenacaoAscendente ? 'asc' : 'desc'}`, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'a40016aabcx9'
        },
    })
    .then(response => {
        if (response.ok) {
            return response.json();
        } else {
            throw new Error('Erro ao atribuir pedidos');
        }
    })
    .then(data => {
            console.log(data)
            const TipoNotaFiltrado1 = data.filter(item => item["03-TipoNota"] !== "39 - BN MPLUS" )
            TipoNotaFiltrado1.forEach(item => {
            item["18-%Reposto"] = parseFloat(item["18-%Reposto"]);
            item["18-%Reposto"] = (item["18-%Reposto"] * 1).toFixed(2); // Multiplica por 100 e formata com 2 casas decimais
            item["18-%Reposto"] += "%";
            item["20-Separado%"] = parseFloat(item["20-Separado%"]);
            item["20-Separado%"] = (item["20-Separado%"] * 1).toFixed(2); // Multiplica por 100 e formata com 2 casas decimais
            item["20-Separado%"] += "%";
            item["14-AgrupamentosPedido"] = item["14-AgrupamentosPedido"].replaceAll('/',',');
            
                });

                    ordenacaoAscendente = !ordenacaoAscendente;

        FecharModalLoading();
        criarTabelaDistribuicao(TipoNotaFiltrado1);
        PintarPedidosCompletos();
        marcarLinhasDuplicadas();
        PintarPedidosUrgentes();
        if (Empresa === "1") {
            CarregarUsuarios(ApiUsuariosMatriz);
        } else if (Empresa === "4") {
            CarregarUsuarios(ApiUsuariosFilial);
        }
        
        })
    .catch(error => {
        console.error(error);
    });
    
}

function capturarPedidoVerificacao() {
    const linhasTabela = document.getElementById('TabelaTabelaDistribuicao').getElementsByTagName('tr');
    const PedidosSelecionados = [];

    for (let i = 1; i < linhasTabela.length; i++) {
        const linha = linhasTabela[i];
        const checkbox = linha.querySelector('input[type="checkbox"]');
        
        if (checkbox.checked) {
            const colunas = linha.getElementsByTagName('td');
            const Pedidos = colunas[11].textContent.trim(); // Substitua o índice (9) pela coluna desejada
            
            // Verifica se há mais de um pedido na string e separa-os em uma lista
            const pedidosSeparados = Pedidos.split(',').map(pedido => pedido.trim());
            
            // Adiciona os pedidos à lista
            PedidosSelecionados.push(...pedidosSeparados);
        }
    }

    if (PedidosSelecionados.length > 1) {
        alert('Você selecionou mais de um pedido. Por favor, verifique os pedidos antes de continuar.');
    }

    return PedidosSelecionados;
}


function PecasFaltantes() {
    const PedidosSelecionados = capturarPedidoVerificacao();
    AbrirModalLoading()
    fetch(`http://192.168.0.183:5000/api/DetalharPedido?codPedido=${PedidosSelecionados}`, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'a40016aabcx9'
        }
    })
    .then(response => {
        if (response.ok) {
            return response.json();
        } else {
            throw new Error('Erro ao atribuir pedidos');
        }
    })
    .then(data => {
        const detalhamentoPedido = data[0]["5- Detalhamento dos Sku:"];
        const Filtro = detalhamentoPedido.filter(item => item["endereco"] === "Não Reposto");
    
        const listaPeçasFaltantes = document.getElementById("ListaPeçasFaltantes");
        listaPeçasFaltantes.innerHTML = ''; // Limpa o conteúdo atual
    
        Filtro.forEach(item => {
            const listItem = document.createElement("li");
            listItem.textContent = `${item["referencia"]} / ${item["tamanho"]} / ${item["cor"]} - ${item["reduzido"]}`;
            listaPeçasFaltantes.appendChild(listItem);
           
        });

        const modal = document.getElementById("ModalPeçasFaltantes");
        const modal2 = document.querySelector(".ModalPeçasFaltantes-content");
        const TituloModal = document.getElementById("TituloModal");
        modal.style.display = "block";
        modal2.style.display = "block";
        TituloModal.textContent = `Peças Faltantes no Pedido: ${PedidosSelecionados}`;
        FecharModalLoading();
    })
    .catch(error => {
        console.error(error);
        FecharModalLoading();
    });
}

const fecharModalBtn = document.querySelector(".close");

fecharModalBtn.addEventListener('click', () => {
    const modal = document.querySelector(".ModalPeçasFaltantes-content");
    modal.style.display = "none";
    const modal2 = document.querySelector(".modal");
    modal2.style.display = "none";
    
});


const BotaoFalta = document.getElementById("VerificarPecasFaltando");

BotaoFalta.addEventListener('click', () => {
    PecasFaltantes();
});

function criarTabelaInformacoes(listaInformacoes) {
    const tabela = document.getElementById('TabelaInformacao');
    tabela.innerHTML = ''; // Limpa o conteúdo da tabela antes de preenchê-la novamente

    // Cria o cabeçalho da tabela
    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const ColunaNome = document.createElement('th');
    const ColunaQtdPedidos = document.createElement('th');
    const ColunaQtdPecas = document.createElement('th');
    const ColunaMediaPecas = document.createElement('th');
    const ColunaValorSugestao = document.createElement('th');
    

    ColunaNome.textContent = 'Nome';
    ColunaQtdPedidos.textContent = 'Qtd. Pedidos';
    ColunaQtdPecas.textContent = 'Qtd. Pecas';
    ColunaMediaPecas.textContent = 'Média Peças';
    ColunaValorSugestao.textContent = 'Valor R$';
  

    cabecalhoRow.appendChild(ColunaNome);
    cabecalhoRow.appendChild(ColunaQtdPedidos);
    cabecalhoRow.appendChild(ColunaQtdPecas);
    cabecalhoRow.appendChild(ColunaMediaPecas);
    cabecalhoRow.appendChild(ColunaValorSugestao);
    cabecalho.appendChild(cabecalhoRow);
    tabela.appendChild(cabecalho);

    // Preenche a tabela com os dados dos usuários
    PedidosExibicao = listaInformacoes.slice(indiceExibicao, indiceExibicao + 99999);

    PedidosExibicao.forEach(item => {
        const row = document.createElement('tr');
        const ColunaNome = document.createElement('td');
        const ColunaQtdPedidos = document.createElement('td');
        const ColunaQtdPecas = document.createElement('td');
        const ColunaMediaPecas = document.createElement('td');
        const ColunaValorSugestao = document.createElement('td');
    
   
        ColunaNome.textContent = item["1- nome"];
        ColunaQtdPedidos.textContent = item["2- qtdPedidos"];
        ColunaQtdPecas.textContent = item["3- qtdepçs"];
        ColunaMediaPecas.textContent = item["4- Méd. pç/pedido"];
        ColunaValorSugestao.textContent = item["5- Valor Atribuido"];
     

        row.appendChild(ColunaNome);
        row.appendChild(ColunaQtdPedidos);
        row.appendChild(ColunaQtdPecas);
        row.appendChild(ColunaMediaPecas);
        row.appendChild(ColunaValorSugestao);
        tabela.appendChild(row);
        });

    };

    const ApiIndicadorDistribuicaoMatriz = 'http://192.168.0.183:5000/api/IndicadorDistribuicao';
    const ApiIndicadorDistribuicaoFilial = 'http://192.168.0.184:5000/api/IndicadorDistribuicao';
  
function PassarInformacoes(api){
    fetch(api, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'a40016aabcx9'
        },
     })
    .then(response => {
        if (response.ok) {
            return response.json();
        } else {
            throw new Error('Erro ao obter a lista de usuários');
        }
    })
    .then(data => {

            console.log(data)
            criarTabelaInformacoes(data)
    })
    .catch(error => {
        console.error(error);
            FecharModalLoading();
    });
}


const ApiRecarregarPedidosMatriz = "http://192.168.0.183:5000/api/RecarregarPedidos?empresa=";
const ApiRecarregarPedidosFilial = "http://192.168.0.184:5000/api/RecarregarPedidos?empresa=";
async function AtualizarDados(api, Empresa) {
    AbrirModalLoading();
    try {
        const response = await fetch(`${api}${Empresa}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'a40016aabcx9'
            },
        });

        if (response.ok) {
            const data = await response.json();
            console.log(data);
            criarTabelaInformacoes(data);
        } else {
            throw new Error('Erro ao obter os dados da API');
        }
    } catch (error) {
        console.error(error);
        FecharModalLoading();
    }
}

const linkSair = document.querySelector('.right-menu-item li a[href="/Login_Teste"]');

linkSair.addEventListener("click" , async () => {
  localStorage.clear();
});

