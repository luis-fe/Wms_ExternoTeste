const Empresa = localStorage.getItem('CodEmpresa');

const rootElement = document.documentElement;

if (Empresa === "1") {
    rootElement.classList.add('palheta-empresa-a');
} else if (Empresa === "4") {
    rootElement.classList.add('palheta-empresa-b');
  } else {
    window.location.href = '/Login_Teste';
}

const ApiMatriz = 'http://192.168.0.183:5000/api/RelatorioInventario?';
const ApiFilial = 'http://192.168.0.184:5000/api/RelatorioInventario?';
  const Token1= 'a40016aabcx9';

    document.getElementById('BotaoConsultar').addEventListener('click', async () => {
        
        const Natureza = document.getElementById('SelectNaturezas').value;
        const DataInicio = document.getElementById('InputDataInicio').value;
        const DataFim = document.getElementById('InputDataFim').value;
        if (Empresa === "1") {
          await ChamadaApiInventarios(ApiMatriz, Natureza, DataInicio, DataFim);
      } else if (Empresa === "4") {
        await ChamadaApiInventarios(ApiFilial, Natureza, DataInicio, DataFim);
      } else {
          window.location.href = 'Login.html';
      }
        
        console.log(Natureza);
        console.log(DataInicio);
        console.log(DataFim);
        

    })
    document.getElementById('ImagemExcel').addEventListener('click', async () => {
        
        const Natureza = document.getElementById('SelectNaturezas').value;
        const DataInicio = document.getElementById('InputDataInicio').value;
        const DataFim = document.getElementById('InputDataFim').value;
        if (Empresa === "1") {
          await ChamadaApiInventariosExel(ApiMatriz, Natureza, DataInicio, DataFim, "True");
      } else if (Empresa === "4") {
        await ChamadaApiInventariosExel(ApiFilial, Natureza, DataInicio, DataFim, "True");
      } else {
          window.location.href = 'Login.html';
      }
        
        
        console.log(Natureza);
        console.log(DataInicio);
        console.log(DataFim);
        

    })




    async function ChamadaApiInventarios(api, natureza, dataInicio, dataFim) {
      try {
        const response = await fetch(`${api}natureza=${natureza}&datainicio=${dataInicio}&datafinal=${dataFim}`, {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': Token1
          },
        });

        if (response.ok) {
          const data = await response.json();
          console.log(data);
          criarTabelaInventario(data[0]['5- Detalhamento Ruas:']);
          document.getElementById("LabelResultPratTotal").textContent = data[0]['3 - Total Enderecos'];
          document.getElementById("LabelResultPratInventariadas").textContent = data[0]['4- Enderecos Inventariados'];
          document.getElementById("LabelResultPecasTotais").textContent = data[0]['1: Total de Peças'];
          document.getElementById("LabelResultPecasInventariadas").textContent = data[0]['2- Pçs Inventariadas'];
          document.getElementById('Infomacoes1').style.display = 'flex';
          document.getElementById('ImagemExcel').style.display = 'flex';
        
        } else {
          throw new Error('Erro No Retorno');
        }
      } catch (error) {
        console.error(error);
      }
    }

    
    async function ChamadaApiInventariosExel(api, natureza, dataInicio, dataFim, Relatorio) {
      try {
        const response = await fetch(`${api}natureza=${natureza}&datainicio=${dataInicio}&datafinal=${dataFim}&emitirRelatorio=${Relatorio}`, {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': Token1
          },
        });

        if (response.ok) {
          const data = await response.json();
          console.log(data);
          exportToExcel(data, 'RelatórioInventário.xlsx', 'Relatório Inventário')
        } else {
          throw new Error('Erro No Retorno');
        }
      } catch (error) {
        console.error(error);
      }
    }
    
async function criarTabelaInventario(listaInventarios) {
    const tabelaInventarios = document.getElementById('TabelaInventarios');
    tabelaInventarios.innerHTML = ''; // Limpa o conteúdo da tabela antes de preenchê-la novamente

    // Cria o cabeçalho da tabela
    const cabecalho = tabelaInventarios.createTHead();
    const cabecalhoLinha = cabecalho.insertRow();

    const cabecalhoCelula1 = cabecalhoLinha.insertCell(0);
    cabecalhoCelula1.innerHTML = 'Rua';
    const cabecalhoCelula2 = cabecalhoLinha.insertCell(1);
    cabecalhoCelula2.innerHTML = 'Quantidade Endereços';
    const cabecalhoCelula3 = cabecalhoLinha.insertCell(2);
    cabecalhoCelula3.innerHTML = 'Status';
    const cabecalhoCelula4 = cabecalhoLinha.insertCell(3);
    cabecalhoCelula4.innerHTML = '% Realizado';

    const corpoTabela = tabelaInventarios.createTBody();

    listaInventarios.forEach(item => {
        const linha = corpoTabela.insertRow();
        const celula1 = linha.insertCell(0);
        celula1.innerHTML = item.Rua;
        const celula2 = linha.insertCell(1);
        celula2.innerHTML = item['Qtd Prat.'];
        const celula3 = linha.insertCell(2);
        celula3.innerHTML = item.status;
        const celula4 = linha.insertCell(3);
        celula4.innerHTML = item['% Realizado'];
    });
}

window.addEventListener('load', async () => {
  const NomeUsuario = localStorage.getItem('nomeUsuario');
  const VerificaLogin = localStorage.getItem('Login');
  const linkUsuario = document.querySelector('.right-menu-item a')

  if (VerificaLogin !== "Logado") {
      // Se não houver token, redirecione para a página de login
      window.location.href = '/Login_Teste';
  } else {
    linkUsuario.textContent = NomeUsuario;
    const inputDataInicio = document.getElementById('InputDataInicio');
    const inputDataFim = document.getElementById('InputDataFim');

    // Obtemos a data atual
    const dataAtual = new Date();

    // Formato da data para preenchimento nas inputs (YYYY-MM-DD)
    const formatoData = 'yyyy-MM-dd';

    // Função para formatar a data
    const formatarData = (data) => {
        const ano = data.getFullYear();
        const mes = String(data.getMonth() + 1).padStart(2, '0'); // Mês começa do zero, então adicionamos 1
        const dia = String(data.getDate()).padStart(2, '0');
        return `${ano}-${mes}-${dia}`;
    };

    // Preenchemos as inputs com a data atual
    inputDataInicio.value = formatarData(dataAtual);
    inputDataFim.value = formatarData(dataAtual);

  }
});




async function exportToExcel(data, fileName, sheetName) {

const ws = XLSX.utils.json_to_sheet(data);
const wb = XLSX.utils.book_new();
await XLSX.utils.book_append_sheet(wb, ws, sheetName || 'Sheet 1');

XLSX.writeFile(wb, fileName || 'exportedData.xlsx');
}

const linkSair = document.querySelector('.right-menu-item li a[href="/Login_Teste"]');

linkSair.addEventListener("click" , async () => {
  localStorage.clear();
});

