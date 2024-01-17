
const ApiDistribuicao = 'http://192.168.0.184:5000/api/FilaPedidos';

async function criarTabelasProdutividade(listaDados, tabela, exibirColunas = false) {
    const tabelaProdutividade = document.getElementById(tabela);
    tabelaProdutividade.innerHTML = '';

    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const ColunaRanking = document.createElement('th');
    const ColunaColaborador = document.createElement('th');
    const ColunaQuantidade = document.createElement('th');
    const ColunaQuantidadePedidos = document.createElement('th');
    const ColunaMediaPedidos = document.createElement('th');
    const ColunaRitmo = document.createElement('th');

    ColunaRanking.textContent = 'Rank';
    ColunaColaborador.textContent = 'Colaborador';
    ColunaQuantidade.textContent = 'Qtd';
    ColunaQuantidadePedidos.textContent = 'Qtd. Ped.';
    ColunaMediaPedidos.textContent = 'Méd. Pçs';
    ColunaRitmo.textContent = 'Ritmo';

    cabecalhoRow.appendChild(ColunaRanking);
    cabecalhoRow.appendChild(ColunaColaborador);
    cabecalhoRow.appendChild(ColunaQuantidade);
    if (exibirColunas === true) {
        cabecalhoRow.appendChild(ColunaQuantidadePedidos);
        cabecalhoRow.appendChild(ColunaMediaPedidos);
    }
    cabecalhoRow.appendChild(ColunaRitmo);
    cabecalho.appendChild(cabecalhoRow);
    tabelaProdutividade.appendChild(cabecalho);

    listaDados.forEach((item, index) => {
        const row = document.createElement('tr');
        const ColunaRanking = document.createElement('td');
        const ColunaColaborador = document.createElement('td');
        const ColunaQuantidade = document.createElement('td');
        const ColunaQuantidadePedidos = document.createElement('td');
        const ColunaMediaPedidos = document.createElement('td');
        const ColunaRitmo = document.createElement('td');

        ColunaRanking.textContent = `${index + 1}º`;
        ColunaColaborador.textContent = item.nome;
        ColunaQuantidade.textContent = item.qtde;
        ColunaQuantidadePedidos.textContent = item["Qtd Pedido"];
        ColunaMediaPedidos.textContent = item["Méd pçs/ped."];
        ColunaRitmo.textContent = item.ritmo;

        row.appendChild(ColunaRanking);
        row.appendChild(ColunaColaborador);
        row.appendChild(ColunaQuantidade);
        if (exibirColunas === true) {
            row.appendChild(ColunaQuantidadePedidos);
            row.appendChild(ColunaMediaPedidos);
        }
        row.appendChild(ColunaRitmo);

        tabelaProdutividade.appendChild(row);
    });
}

function formatarMoeda(valor) {
    const formatoMoeda = {
        style: "currency",
        currency: "BRL"
    };
    return valor.toLocaleString("pt-BR", formatoMoeda);
}





async function DadosFaturamento(dataInicio, dataFim, ) {
    try {
        const response = await fetch(`http://192.168.0.184:5000/api/Faturamento?empresa=4&dataInicio=${dataInicio}&dataFim=${dataFim}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'a40016aabcx9'
            },
        });

        if (response.ok) {
            const data = await response.json();
            const Faturado = data[0]['Total Faturado'];
            const RetornaPecas = data[0]['Pcs Retorna'];
            const Retorna = data[0]['No Retorna'];
            const RetornaMplus = data[0]['No Retorna MPlus'];
            const RetornaMplusPcs = data[0]['Pcs Retorna Mplus'];
            const ProntaEntrega = data[0]['Pç Pronta Entrega'];
            const ProntaEntregaR$ = data[0]['Retorna ProntaEntrega'];
            document.getElementById("FaturadoR$").textContent = `Faturado R$: ${Faturado}`  ;
            document.getElementById("RetornaPcs").textContent = `Retorna Pçs: ${RetornaPecas}`;
            document.getElementById("RetornaValor").textContent = `Retorna R$: ${formatarMoeda(Retorna)}`;
            document.getElementById("RetornaMplus").textContent = `Retorna Mplus Pçs: ${RetornaMplusPcs}`;
            document.getElementById("RetornaMplusR$").textContent = `Retorna Mplus R$: ${formatarMoeda(RetornaMplus)}`;
            document.getElementById("RetornaProntaEnt").textContent = `Pronta Entrega Pcs: ${ProntaEntrega}`;
            document.getElementById("RetornaProntaEntR$").textContent = `Pronta Entrega R$: ${formatarMoeda(ProntaEntregaR$)}`;
        } else {
            throw new Error("Erro na Atualização, Recarregue a página!\nSe o problema persistir, contate o Administrador!");
        }
    } catch (error) {
        console.error(error);
        alert(error.message);
    }
}

async function tabelaProdutividade(Consulta, Tabela, dataIni, DataFim, NomeRecorde, QtdRecorde, Qtd,horaInicio, horaFim, situacaoColuna) {
    try {
        const response = await fetch(`http://192.168.0.184:5000/api/${Consulta}/Resumo?DataInicial=${dataIni}&DataFinal==${DataFim}&horarioInicial=${horaInicio}&horarioFinal=${horaFim}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'a40016aabcx9'
            },
        });

        if (response.ok) {
            const data = await response.json();
            const detalhamentoRanking = data[0]['3- Ranking Repositores'];
            const detalhamentoNomeRecord = data[0]["1- Record Repositor"];
            const detalhamentoQtdRecord = data[0]["1.1- Record qtd"];
            const Total = data[0]["2 Total Periodo"];
            criarTabelasProdutividade(detalhamentoRanking, Tabela, situacaoColuna);
            document.getElementById(NomeRecorde).textContent = detalhamentoNomeRecord;
            document.getElementById(QtdRecorde).textContent = `${detalhamentoQtdRecord} Pçs`
            document.getElementById(Qtd).textContent = `${parseFloat(Total)} Pçs`;
        } else {
            throw new Error("Erro na Atualização, Recarregue a página!\nSe o problema persistir contacte o Administrador!");
        }
    } catch (error) {
        console.error(error);
    }
}

function getFormattedDate(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

const botaoFiltrar = document.getElementById("btnFiltrar")

botaoFiltrar.addEventListener('click', () => {
    atualizarTabelas();
    document.getElementById("btnFiltrar").disabled = true;
})

let horarioInicioInput;
let horarioFinalInput;
window.addEventListener('load', () => {
    const currentDate = new Date();
    const formattedDate = getFormattedDate(currentDate);
    horarioInicioInput = document.getElementById("HorarioInicio");
    horarioFinalInput = document.getElementById("HorarioFinal");


    horarioInicioInput.value = "07:30";
    horarioFinalInput.value = "18:00";

    tabelaProdutividade("TagsReposicao", "TAbelaRepositor", formattedDate, formattedDate, "NomeRecordeRep", "ValorRecordeRep", "LabelTotalReposto",horarioInicioInput.value, horarioFinalInput.value, false);
    tabelaProdutividade("TagsSeparacao", "TAbelaSeparador", formattedDate, formattedDate, "NomeRecordeSep", "ValorRecordeSep", "LabelTotalSeparado", horarioInicioInput.value, horarioFinalInput.value, true);
    DadosFaturamento(formattedDate, formattedDate);
    console.log(horarioFinalInput.value)
    document.getElementById("DataInicial").value = formattedDate
    document.getElementById("DataFinal").value = formattedDate
    setTimeout(atualizarTabelas, 10000);
});

async function atualizarTabelas() {
    const DataIni = document.getElementById("DataInicial").value;
    const DataFin = document.getElementById("DataFinal").value;

    try {
        await Promise.all([
            tabelaProdutividade("TagsReposicao", "TAbelaRepositor", DataIni, DataFin, "NomeRecordeRep", "ValorRecordeRep", "LabelTotalReposto",horarioInicioInput.value, horarioFinalInput.value, false),
            tabelaProdutividade("TagsSeparacao", "TAbelaSeparador", DataIni, DataFin, "NomeRecordeSep", "ValorRecordeSep", "LabelTotalSeparado",horarioInicioInput.value, horarioFinalInput.value, true)
        ]);
        criarTabelasProdutividade(dataReposicao, "TAbelaRepositor", false);
        criarTabelasProdutividade(dataSeparacao, "TAbelaSeparador", true);
        document.getElementById("btnFiltrar").disabled = false;
    } catch (error) {
        console.error(error);
        document.getElementById("btnFiltrar").disabled = false;
    }

    DadosFaturamento(DataIni,DataFin);
    setTimeout(atualizarTabelas, 60000);
}
