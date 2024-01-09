const Empresa = localStorage.getItem('CodEmpresa');

const rootElement = document.documentElement;

if (Empresa === "1") {
    rootElement.classList.add('palheta-empresa-a');
} else if (Empresa === "4") {
    rootElement.classList.add('palheta-empresa-b');
} else {
    window.location.href = '/Login_Teste';
}


const apiCadastroMatriz= 'http://192.168.0.183:5000/api/CadastrarCaixa';
const apiCadastroFilial = 'http://177.221.240.74:5000/api/CadastrarCaixa';
const apiConsultaFilial = "http://177.221.240.74:5000/api/relatorioCaixas";
const apiConsultaMatriz = "http://192.168.0.183:5000/api/relatorioCaixas";
const Token = "a40016aabcx9";

function getFormattedDate(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

async function obterInformacoesCaixas(api,dataInicio, dataFim) {
  
    try {
        const response = await fetch(`${api}?dataInicio=${dataInicio}&dataFim=${dataFim}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': Token
            },
        });

        if (response.ok) {
            const data = await response.json();
            console.log(data);
            criarTabelaEmbalagens(data);
        } else {
            throw new Error('Erro ao obter os dados da API');
        }
    } catch (error) {
        console.error(error);
    
    }
}

async function InserirCaixa(api) {
    const Salvar = {
        "codcaixa": document.getElementById('InputCodigo').value,
        "nomecaixa": document.getElementById('InputDescricaoEmbalagem').value,
        "tamanhocaixa": document.getElementById('InputTamanhoCaixa').value
    };

    try {
        const response = await fetch(api, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': Token
            },
            body: JSON.stringify(Salvar),
        });

        if (response.ok) {
            const data = await response.json();
            const resultado = data["Mensagem"];
            console.log(resultado);
        } else {
            throw new Error('Erro No Retorno');
        }
    } catch (error) {
        console.error(error);
    }
}



    window.addEventListener('load', async () => {
        const currentDate = new Date();
        const formattedDate = getFormattedDate(currentDate);
    
        document.getElementById("DataInicial").value = formattedDate;
        document.getElementById("DataFinal").value = formattedDate;

        const VerificaLogin = localStorage.getItem('Login');

        if (VerificaLogin !== "Logado") {
            // Se não houver token, redirecione para a página de login
            window.location.href = '/Login_Teste';
        }
        else 
        if (Empresa === "1") {
            obterInformacoesCaixas( apiConsultaMatriz,document.getElementById("DataInicial").value, document.getElementById("DataFinal").value);
        } else if (Empresa === "4") {
            obterInformacoesCaixas(apiConsultaFilial, document.getElementById("DataInicial").value, document.getElementById("DataFinal").value);
        }


    
 

    
    
});

document.getElementById('btnCadastrar').addEventListener('click', () => {
    document.getElementById('ModalCadastroEmbalagem').style.display = 'flex';
    console.log('TESTE'); // Corrigido o erro de digitação aqui (era "conseole.log")
});

const inputCodigo = document.getElementById("InputCodigo");
const inputDescricaoEmbalagem = document.getElementById("InputDescricaoEmbalagem");
const inputTamanhoCaixa = document.getElementById("InputTamanhoCaixa");

document.getElementById('SalvarNovaCaixa').addEventListener('click', () => {
    const modalEmbalagemContent = document.querySelector('.ModalCadastroEmbalagem-content');
    const inputs = modalEmbalagemContent.querySelectorAll('input[type="text"], input[type="password"]');
    let FaltaInformacaoNovo = false;

    inputs.forEach(input => {
        if (input.value === '') {
            FaltaInformacaoNovo = true;
            input.style.borderColor = 'red';
            setTimeout(() => {
                input.style.borderColor = '';
            }, 10000);
            console.log(`Input ${input.id} não está preenchido`);
        }
    });

    if (!FaltaInformacaoNovo) {
        const confirmationBox = document.getElementById('confirmationBox');
        confirmationBox.style.display = 'block'; // Exibir a caixa de confirmação
    }
});

document.getElementById('confirmButton').addEventListener('click', () => {
    const modalEmbalagemContent = document.querySelector('.ModalCadastroEmbalagem-content');
    const inputs = modalEmbalagemContent.querySelectorAll('input[type="text"], input[type="password"]');


    if (Empresa === "1") {
        InserirCaixa(apiCadastroMatriz);
    } else if (Empresa === "4") {
        InserirCaixa(apiCadastroFilial);
    }
    

    // Ocultar a caixa de confirmação após a confirmação
    const confirmationBox = document.getElementById('confirmationBox');
    confirmationBox.style.display = 'none';
    inputs.forEach(input => {
        input.value = '';
    });
    document.getElementById('ModalCadastroEmbalagem').style.display = 'none';
});

document.getElementById('cancelButton').addEventListener('click', () => {
    // Ocultar a caixa de confirmação se o usuário cancelar
    const confirmationBox = document.getElementById('confirmationBox');
    confirmationBox.style.display = 'none';
});

function criarTabelaEmbalagens(listaChamados) {
    const tabelaEmbalagens = document.getElementById('TabelaCaixa');
    tabelaEmbalagens.innerHTML = ''; // Limpa o conteúdo da tabela antes de preenchê-la novamente

    // Cria o cabeçalho da tabela
    const cabecalho = tabelaEmbalagens.createTHead();
    const cabecalhoLinha = cabecalho.insertRow();

    const cabecalhoCelula1 = cabecalhoLinha.insertCell(0);
    cabecalhoCelula1.innerHTML = 'Codigo Caixa';
    const cabecalhoCelula2 = cabecalhoLinha.insertCell(1);
    cabecalhoCelula2.innerHTML = 'Tamanho Caixa';
    const cabecalhoCelula3 = cabecalhoLinha.insertCell(2);
    cabecalhoCelula3.innerHTML = 'Quantidade';

    const corpoTabela = tabelaEmbalagens.createTBody();

    listaChamados.forEach(item => {
        const linha = corpoTabela.insertRow();
        const celula1 = linha.insertCell(0);
        celula1.innerHTML = item.codcaixa;
        const celula2 = linha.insertCell(1);
        celula2.innerHTML = item.tamcaixa;
        const celula3 = linha.insertCell(2);
        celula3.innerHTML = item.quantidade;
    });
}


document.getElementById("btnFiltrar").addEventListener('click', ()=> {
    if (Empresa === "1") {
        obterInformacoesCaixas( apiConsultaMatriz,document.getElementById("DataInicial").value, document.getElementById("DataFinal").value);
    } else if (Empresa === "4") {
        obterInformacoesCaixas(apiConsultaFilial, document.getElementById("DataInicial").value, document.getElementById("DataFinal").value);
    }
})

document.getElementById('FecharModalCadastro').addEventListener('click', () => {
    const modalEmbalagemContent = document.querySelector('.ModalCadastroEmbalagem-content');
    const inputs = modalEmbalagemContent.querySelectorAll('input[type="text"], input[type="password"]');

    // Ocultar a caixa de confirmação após a confirmação
    const confirmationBox = document.getElementById('confirmationBox');
    confirmationBox.style.display = 'none';
    inputs.forEach(input => {
        input.value = '';
    });
    document.getElementById('ModalCadastroEmbalagem').style.display = 'none'
})

const linkSair = document.querySelector('.right-menu-item li a[href="/Login_Teste"]');

linkSair.addEventListener("click" , async () => {
  localStorage.clear();
});
