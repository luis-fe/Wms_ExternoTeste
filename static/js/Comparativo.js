const Empresa = localStorage.getItem('CodEmpresa');

const rootElement = document.documentElement;

if (Empresa === "1") {
    rootElement.classList.add('palheta-empresa-a');
} else if (Empresa === "4") {
    rootElement.classList.add('palheta-empresa-b');
} else {
    window.location.href = 'Login.html';
}


const ApiComparativoMatriz = "http://192.168.0.183:5000/api/confrontoTags"; 
const ApiComparativoFilial = "http://192.168.0.184:5000/api/confrontoTags";
const Token = "a40016aabcx9"

async function ChamadaApiComparativo(api) {
    try {
      const response = await fetch(`${api}`, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': Token
        },
      });

      if (response.ok) {
        const data = await response.json();
        console.log(data);
        criarTabelaComparativos(data[0]['4- Detalhamento ']);
        
      
      } else {
        throw new Error('Erro No Retorno');
      }
    } catch (error) {
      console.error(error);
    }
  }




async function criarTabelaComparativos(listaInventarios) {
    const tabelaInventarios = document.getElementById('TabelaComparativo');
    tabelaInventarios.innerHTML = ''; // Limpa o conteúdo da tabela antes de preenchê-la novamente

    // Cria o cabeçalho da tabela
    const cabecalho = tabelaInventarios.createTHead();
    const cabecalhoLinha = cabecalho.insertRow();

    const cabecalhoCelula1 = cabecalhoLinha.insertCell(0);
    cabecalhoCelula1.innerHTML = 'Reduzido';
    const cabecalhoCelula2 = cabecalhoLinha.insertCell(1);
    cabecalhoCelula2.innerHTML = 'Tags em Conferência';
    const cabecalhoCelula3 = cabecalhoLinha.insertCell(2);
    cabecalhoCelula3.innerHTML = 'Tags WMS';
    const cabecalhoCelula4 = cabecalhoLinha.insertCell(3);
    cabecalhoCelula4.innerHTML = 'Tags Posição Estoque';
    const cabecalhoCelula5 = cabecalhoLinha.insertCell(4);
    cabecalhoCelula5.innerHTML = 'Diferença';

    const corpoTabela = tabelaInventarios.createTBody();

    listaInventarios.forEach(item => {
        const linha = corpoTabela.insertRow();
        const celula1 = linha.insertCell(0);
        celula1.innerHTML = item.reduzido;
        const celula2 = linha.insertCell(1);
        celula2.innerHTML = item['em_conferencia'];
        const celula3 = linha.insertCell(2);
        celula3.innerHTML = item.situacao3;
        const celula4 = linha.insertCell(3);
        celula4.innerHTML = item['posicao_estoque'];
        const celula5 = linha.insertCell(4);
        celula5.innerHTML = item.diferenca;
    });
}

window.addEventListener('load', async () => {
    const NomeUsuario = localStorage.getItem('nomeUsuario');
    const VerificaLogin = localStorage.getItem('Login');
    const linkUsuario = document.querySelector('.right-menu-item a');

    if (Empresa === "1") {
        if (VerificaLogin !== "Logado") {

            window.location.href = 'Login.html';
        } else {
            linkUsuario.textContent = NomeUsuario;
            await ChamadaApiComparativo(ApiComparativoMatriz);

            
        }
    } else if (Empresa === "4") {
        if (VerificaLogin !== "Logado") {
            window.location.href = 'Login.html';
        } else {
            linkUsuario.textContent = NomeUsuario;
            await ChamadaApiComparativo(ApiComparativoFilial);
        }
    }
});

const linkSair = document.querySelector('.right-menu-item li a[href="Login_Teste.html"]');

linkSair.addEventListener("click" , async () => {
  localStorage.clear();
});
