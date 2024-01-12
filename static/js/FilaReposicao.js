const Empresa = localStorage.getItem('CodEmpresa');

const rootElement = document.documentElement;

if (Empresa === "1") {
    rootElement.classList.add('palheta-empresa-a');
} else if (Empresa === "4") {
    rootElement.classList.add('palheta-empresa-b');
} else {
    window.location.href = '/Login_Teste';
}

const ApiFilaMatriz = 'http://192.168.0.183:5000/api/NecessidadeReposicao';
const ApiFilaFilial = 'http://192.168.0.184:5000/api/NecessidadeReposicao'
const Token = "a40016aabcx9"
let dadosApi = ""

function criarTabelaFilaReposicao(listaFila) {
    const TabelaFilaReposicao = document.getElementById('TabelaFilaReposicao');
    TabelaFilaReposicao.innerHTML = ''; 

    // Cria o cabeçalho da tabela
    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const Engenharia = document.createElement('th');
    const codReduzido = document.createElement('th');
    const Necessidade = document.createElement('th');
    const SaldoFila = document.createElement('th');
    const Op = document.createElement('th');
    const PedidosFaltantes = document.createElement('th');
    const Epc = document.createElement('th');

    
    Engenharia.textContent = 'Cód. Engenharia';
    codReduzido.textContent = 'Cód. Reduzido';
    Necessidade.textContent = 'Necessidade Peças';
    SaldoFila.textContent = 'Saldo em Fila';
    Op.textContent = "Op's";
    PedidosFaltantes.textContent = 'Qtd. Pedidos Faltando';
    Epc.textContent = 'Cód Epc Referencial';


    cabecalhoRow.appendChild(Engenharia);
    cabecalhoRow.appendChild(codReduzido);
    cabecalhoRow.appendChild(Necessidade);
    cabecalhoRow.appendChild(SaldoFila);
    cabecalhoRow.appendChild(Op);
    cabecalhoRow.appendChild(PedidosFaltantes);
    cabecalhoRow.appendChild(Epc);
    cabecalho.appendChild(cabecalhoRow);
    TabelaFilaReposicao.appendChild(cabecalho);


    listaFila.forEach(item => {
        const row = document.createElement('tr');
        const Engenharia = document.createElement('td');
        const codReduzido = document.createElement('td');
        const Necessidade = document.createElement('td');
        const SaldoFila = document.createElement('td');
        const Op = document.createElement('td')
        const PedidosFaltantes = document.createElement('td');
        const Epc = document.createElement('td');

       
        Engenharia.textContent = item["engenharia"];
        codReduzido.textContent = item["codreduzido"];
        Necessidade.textContent = item["Necessidade p/repor"];
        SaldoFila.textContent = item["saldofila"]
        Op.textContent = item["ops"]
        PedidosFaltantes.textContent = item["Qtd_Pedidos que usam"];
        Epc.textContent = item["epc_referencial"];
    
        row.appendChild(Engenharia);
        row.appendChild(codReduzido);
        row.appendChild(Necessidade);
        row.appendChild(SaldoFila);
        row.appendChild(Op);
        row.appendChild(PedidosFaltantes);
        row.appendChild(Epc);
        TabelaFilaReposicao.appendChild(row);
        });

    }

async function ChamadaApi(api){
    try {
        const response = await fetch(api, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': Token
            },
        });
    
        if (response.ok) {
            const data = await response.json();
            const resultado = data[0]["1- Detalhamento das Necessidades "]
            console.log(resultado)
            criarTabelaFilaReposicao(resultado)
            dadosApi = resultado
        } else {
            throw new Error('Erro No Retorno');
        }
    } catch (error) {
        console.error(error);
    }
};


document.getElementById('exportButton').addEventListener('click', function () {
    const nomeArquivo = 'Fila Reposição.xlsx';
      const wb = XLSX.utils.book_new();
      const ws = XLSX.utils.json_to_sheet(dadosApi);

      // Adicionar a planilha ao workbook
      XLSX.utils.book_append_sheet(wb, ws, 'Fila Reposição');

      // Salvar o arquivo
      XLSX.writeFile(wb, nomeArquivo);
  });
  







window.addEventListener('load', () => {
    const VerificaLogin = localStorage.getItem('Login');
    if (Empresa === "1") {
        if (VerificaLogin !== "Logado") {

            window.location.href = '/Login_Teste';
        } else {
            ChamadaApi(ApiFilaMatriz);
        }
    } else if (Empresa === "4") {
        if (VerificaLogin !== "Logado") {
            window.location.href = '/Login_Teste';
        } else {
            ChamadaApi(ApiFilaFilial)
        }
    }
});

const linkSair = document.querySelector('.right-menu-item li a[href="/Login_Teste"]');

linkSair.addEventListener("click" , async () => {
  localStorage.clear();
});

