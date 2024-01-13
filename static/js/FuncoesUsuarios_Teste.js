const Empresa = localStorage.getItem('CodEmpresa');

const rootElement = document.documentElement;

if (Empresa === "1") {
    rootElement.classList.add('palheta-empresa-a');
} else if (Empresa === "4") {
    rootElement.classList.add('palheta-empresa-b');
} else {
    window.location.href = '/Login_Teste';
}

const ApiUsuariosMatriz = "http://192.168.0.183:5000/api/Usuarios"
const ApiUsuariosFilial = "http://192.168.0.184:5000/api/Usuarios"
let Usuario = document.getElementById("")
let UsuarioSelecionadoTabela;
let NomeSelecionadoTabela;
let FuncaoSelecionadoTabela;
let SituacaoSelecionadoTabela;

async function UsuariosWMS(Api) {
    AbrirModalLoading();

    try {
        const response = await fetch(Api, {
            method: "GET",
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'a40016aabcx9'
            },
        });

        if (response.ok) {
            const data = await response.json();
            criarTabelaUsuarios(data);
            console.log(data);
            FecharModalLoading();
        } else {
            throw new Error('Erro ao obter a lista de usuários');
        }
    } catch (error) {
        console.error(error);
        FecharModalLoading();
    }
}


function criarTabelaUsuarios(listaUsuarios) {
    const tabelaUsuarios = document.getElementById('TabelaUsuarios1');
    tabelaUsuarios.innerHTML = ''; // Limpa o conteúdo da tabela antes de preenchê-la novamente

    // Cria o cabeçalho da tabela
    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const colunaCheckbox = document.createElement('th');
    const ColunaLogin = document.createElement('th');
    const ColunaNome = document.createElement('th');
    const ColunaFuncao = document.createElement('th');
    const ColunaSituacao = document.createElement('th');

    
    colunaCheckbox.textContent = '';
    ColunaLogin.textContent = 'Código Usuário';
    ColunaNome.textContent = 'Nome Usuário';
    ColunaFuncao.textContent = 'Funcao';
    ColunaSituacao.textContent = 'Situacao';

    cabecalhoRow.appendChild(colunaCheckbox);
    cabecalhoRow.appendChild(ColunaLogin);
    cabecalhoRow.appendChild(ColunaNome);
    cabecalhoRow.appendChild(ColunaFuncao);
    cabecalhoRow.appendChild(ColunaSituacao);
    cabecalho.appendChild(cabecalhoRow);
    tabelaUsuarios.appendChild(cabecalho);


    listaUsuarios.forEach(item => {
        const row = document.createElement('tr');
        const colunaCheckbox = document.createElement('td');
        const ColunaLogin = document.createElement('td');
        const ColunaNome = document.createElement('td');
        const ColunaFuncao = document.createElement('td');
        const ColunaSituacao = document.createElement('td');

        const checkboxUsuarios = document.createElement('input');
        checkboxUsuarios.type = 'checkbox'; // Alterado de 'radio' para 'checkbox'
        checkboxUsuarios.value = item["codigo"];
        checkboxUsuarios.name = 'checkboxUsuarios';
        checkboxUsuarios.addEventListener('change', function(event) {
            const checkboxes = document.getElementsByName('checkboxUsuarios');
            checkboxes.forEach(box => {
              if (box !== event.target) {
                box.checked = false;
              }
            });
          });
        colunaCheckbox.appendChild(checkboxUsuarios);
        ColunaLogin.textContent = item["codigo"];
        ColunaNome.textContent = item["nome"];
        ColunaFuncao.textContent = item["funcao"];
        ColunaSituacao.textContent = item["situacao"];
    
        row.appendChild(colunaCheckbox);
        row.appendChild(ColunaLogin);
        row.appendChild(ColunaNome);
        row.appendChild(ColunaFuncao);
        row.appendChild(ColunaSituacao);
        tabelaUsuarios.appendChild(row);
        });

    }

const modalLoading = document.getElementById("ModalLoading");

function AbrirModalLoading() {
    modalLoading.style.display = "block";
}

function FecharModalLoading() {
    modalLoading.style.display = "none";
}


window.addEventListener('load', async ()  => {
    const NomeUsuario = localStorage.getItem('nomeUsuario');
    const VerificaLogin = localStorage.getItem('Login');
    const linkUsuario = document.querySelector('.right-menu-item a');
    if (Empresa === "1") {
        if (VerificaLogin !== "Logado") {

            window.location.href = 'Login.html';
        } else {
            linkUsuario.textContent = NomeUsuario;
            await UsuariosWMS(ApiUsuariosMatriz)
        }
    } else if (Empresa === "4") {
        if (VerificaLogin !== "Logado") {
            window.location.href = 'Login.html';
        } else {
            linkUsuario.textContent = NomeUsuario;
            await UsuariosWMS(ApiUsuariosFilial)

        }
    }
});   


const InputBuscaUsuario = document.getElementById('InputBuscaUsuario');
const TabelaUsuarios2 = document.getElementById('TabelaUsuarios1');
        
InputBuscaUsuario.addEventListener('keyup', () => {
    const expressaoUsuarios = InputBuscaUsuario.value.trim().toLowerCase();
    const linhasTabelaUsuarios = TabelaUsuarios2.getElementsByTagName('tr');
        
        for (let i = 1; i < linhasTabelaUsuarios.length; i++) {
            const linhaUsuarios = linhasTabelaUsuarios[i];
            const colunasUsuarios = linhaUsuarios.getElementsByTagName('td');
            let encontrouUsuarios = false;
        
        for (let j = 1; j < colunasUsuarios.length; j++) {
            const conteudoColunaUsuarios = colunasUsuarios[j].textContent.trim().toLowerCase();
        
        if (conteudoColunaUsuarios.includes(expressaoUsuarios)) {
            encontrouUsuarios = true;
            break;
            }
        }
        
        if (encontrouUsuarios) {
            linhaUsuarios.style.display = '';
        } else {
            linhaUsuarios.style.display = 'none';
        }
        }
    });


const BotaoAdicionarUsuario = document.getElementById("AdicionarUsuario");
const AbrirModalNovoUsuario = document.getElementById("ModalNovoUsuario");
const FecharModalNovoUsuario = document.getElementById("FecharModalNovoUsuario");

BotaoAdicionarUsuario.addEventListener('click', () => {
    AbrirModalNovoUsuario.style.display = "block";
});

FecharModalNovoUsuario.addEventListener('click', () => {
    document.getElementById("InputNovoUsuario").value = ""
    document.getElementById("InputNomeNovoUsuario").value = ""
    document.getElementById("InputSenhaNovoUsuario").value = ""
    document.getElementById("SituacaoNovoUsuario").value = ""
    document.getElementById("FuncaoNovoUsuario").value = ""
    AbrirModalNovoUsuario.style.display = "none";
});



const BotaoSalvarNovoUsuario = document.getElementById("salvarNovoUsuario");
let CodUsuario;
let NomeUsuario;
let SenhaUsuario;
let SituacaoUsuario;
let FuncaoUsuario;

function ModificacoesUsuarios (codigo, nome, senha, situacao, funcao) {
CodUsuario = document.getElementById(codigo);
NomeUsuario = document.getElementById(nome);
SenhaUsuario = document.getElementById(senha);
SituacaoUsuario = document.getElementById(situacao);
FuncaoUsuario = document.getElementById(funcao);

}

BotaoSalvarNovoUsuario.addEventListener('click', () => {
    ModificacoesUsuarios("InputNovoUsuario", "InputNomeNovoUsuario", "InputSenhaNovoUsuario", "SituacaoNovoUsuario", "FuncaoNovoUsuario")

    if(SituacaoUsuario.value === "" && FuncaoUsuario.value === "") {
        SituacaoUsuario.style.borderColor = "red";
        FuncaoUsuario.style.borderColor = "red";
        return;
    };

    if(SituacaoUsuario.value === "") {
        SituacaoUsuario.style.borderColor = "red";
        return;
    };

    if(FuncaoUsuario.value === "") {
        FuncaoUsuario.style.borderColor = "red";
        return;
    };


    dadosSalvar = {
        "codigo": parseInt(CodUsuario.value),
        "nome": NomeUsuario.value,
        "funcao": FuncaoUsuario.value,
        "senha": SenhaUsuario.value,
        "situacao": SituacaoUsuario.value
    }

    AlteracoesUsuarios(dadosSalvar, "PUT", "", "", AbrirModalNovoUsuario)

    if (Empresa === "1") {
        AlteracoesUsuarios(dadosSalvar, "PUT", ApiUsuariosMatriz, "", "", AbrirModalNovoUsuario);
    } else if (Empresa === "4") {
        AlteracoesUsuarios(dadosSalvar, "PUT", ApiUsuariosFilial, "", "", AbrirModalNovoUsuario);
    
    }

});


const AbrirModalEditarUsuario = document.getElementById("ModaoEditarUsuarios");
const FecharModalEditarUsuario= document.getElementById("FecharModalEditarUsuario");
const BotaoEditarUsuario = document.getElementById("EditarUsuario");
const InputUsuarioSelecionado = document.getElementById("InputEdicaoUsuario");
BotaoEditarUsuario.addEventListener('click', () => {
    
    const usuarioSelecionado = CheckboxSelecionada();
    if (!usuarioSelecionado) {
        alert("Nenhum Usuário Selecionado");
    } else {
        AbrirModalEditarUsuario.style.display = "block"
        document.getElementById("InputEdicaoUsuario").value = UsuarioSelecionadoTabela;
        InputUsuarioSelecionado.disabled = true;
        document.getElementById("InputNomeEdicaoUsuario").value = NomeSelecionadoTabela;
        document.getElementById("SituacaoEdicaoUsuario").value = SituacaoSelecionadoTabela;
        document.getElementById("FuncaoEdicaoUsuario").value = FuncaoSelecionadoTabela;
    }
});

FecharModalEditarUsuario.addEventListener('click', () => {
    InputUsuarioSelecionado.disabled = false;
    AbrirModalEditarUsuario.style.display = "none"
})



const BotaoSalvarEdicaoUsuario = document.getElementById("salvarEdicaoUsuario");

BotaoSalvarEdicaoUsuario.addEventListener('click', () => {
    ModificacoesUsuarios("InputEdicaoUsuario", "InputNomeEdicaoUsuario", "SituacaoEdicaoUsuario", "SituacaoEdicaoUsuario", "FuncaoEdicaoUsuario")

    const dadosEdicao = {
        "nome": NomeUsuario.value,
        "funcao": FuncaoUsuario.value,
        "situacao": SituacaoUsuario.value
    }

 
    if (Empresa === "1") {
        AlteracoesUsuarios(dadosEdicao, "POST",ApiUsuariosMatriz, "/", parseInt(CodUsuario.value), AbrirModalEditarUsuario);
    } else if (Empresa === "4") {
        AlteracoesUsuarios(dadosEdicao, "POST", ApiUsuariosFilial, "/", parseInt(CodUsuario.value), AbrirModalEditarUsuario);
    
    }

})

async function AlteracoesUsuarios (dados, metodo, api, api1,api2, fecharmodal){

    const apiUrl = `${api}${api1}${api2}`;

    try {
        const response = await fetch(apiUrl, {
            method: metodo,
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'a40016aabcx9'
            },
            body: JSON.stringify(dados),
        });

        if (response.ok) {
            const data = await response.json();
            console.log(data);

            if (Empresa === "1") {
                UsuariosWMS(ApiUsuariosMatriz);
            } else if (Empresa === "4") {
                UsuariosWMS(ApiUsuariosFilial);
            }

            fecharmodal.style.display = "none";
        } else {
            throw new Error('Erro ao obter a lista de usuários');
        }
    } catch (error) {
        console.error(error);
        FecharModalLoading();
    }
}



function CheckboxSelecionada() {
    const tabela = document.getElementById('TabelaUsuarios1');
    const linhas = tabela.getElementsByTagName('tr');

    for (let i = 1; i < linhas.length; i++) {
        const linha = linhas[i];
        const checkbox = linha.querySelector('input[type="checkbox"]:checked');

        if (checkbox) {
            const colunas = linha.getElementsByTagName('td');
            UsuarioSelecionadoTabela = colunas[1].textContent.trim();
            NomeSelecionadoTabela = colunas[2].textContent.trim();
            FuncaoSelecionadoTabela = colunas[3].textContent.trim();
            SituacaoSelecionadoTabela = colunas[4].textContent.trim();
            return UsuarioSelecionadoTabela; // Retorna o valor da variável
        } else {
            UsuarioSelecionadoTabela = "";
            NomeSelecionadoTabela = "";
            FuncaoSelecionadoTabela = "";
            SituacaoSelecionadoTabela = "";
        }
    }

    return null; // Retorna null se nenhuma linha estiver selecionada
}


const linkSair = document.querySelector('.right-menu-item li a[href="/Login_Teste"]');

linkSair.addEventListener("click" , async () => {
  localStorage.clear();
});

