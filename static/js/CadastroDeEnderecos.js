const Empresa = localStorage.getItem('CodEmpresa');

const rootElement = document.documentElement;

if (Empresa === "1") {
    rootElement.classList.add('palheta-empresa-a');
} else if (Empresa === "4") {
    rootElement.classList.add('palheta-empresa-b');
} else {
    window.location.href = '/Login_Teste';
}

function AbrirModalLoading() {
    ModalLoading.style.display = "block";
  }
  
  // Função para fechar a modal de carregamento
  function FecharModalLoading() {
    ModalLoading.style.display = "none";
    document.getElementById('loader').value = '';
  }


  const CadastrarEnderecosMatriz = "http://192.168.0.183:5000/api/EnderecoAtacado";
  const CadastrarEnderecosFilial = "http://192.168.0.184:5000/api/EnderecoAtacado";

function CadastrarEnderecos(api, Metodo, Condicao) {
    AbrirModalLoading();

    const ruaInicial = document.getElementById("inputRuaInicial");
    const ruaFinal = document.getElementById("inputRuaFinal");
    const ModuloInicial = document.getElementById("InputModuloInicial");
    const ModuloFinal = document.getElementById("InputModuloFinal");
    const PosicaoInicial = document.getElementById("InputPosicaoInicial");
    const PosicaoFinal = document.getElementById("InputPosicaoFinal");
    const Natureza = document.getElementById("OpcoesNaturezas");
    const TipoEstoque = document.getElementById("OpcoesEstoque");

    const Dados = {
        "ruaInicial": ruaInicial.value,
        "ruaFinal": ruaFinal.value,
        "modulo": ModuloInicial.value,
        "moduloFinal": ModuloFinal.value,
        "posicao": PosicaoInicial.value,
        "posicaoFinal": PosicaoFinal.value,
        "tipo": TipoEstoque.value,
        "natureza": Natureza.value,
        "empresa": Empresa,
        "imprimir": Condicao
    }

    
    fetch(api, {
        
        method: Metodo,
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'a40016aabcx9'
        },
        body: JSON.stringify(Dados),
    })
    
        .then(response => {
            if (response.ok) {
                return response.json();
            } else {
                throw new Error('Erro ao obter a lista de usuários');
            }
        })
        .then(data => {
            console.log(data);
            FecharModalLoading();
        })
        .catch(error => {
            console.error(error);
            FecharModalLoading();
        });
}



const BotaoSelecionar = document.getElementById("ButtonCad");
const Natureza = document.getElementById("OpcoesNaturezas");
const TipoEstoque = document.getElementById("OpcoesEstoque");
const radioIncluir = document.getElementById("radioIncluir");
const radioExcluir = document.getElementById("radioExcluir");
const BotaoPersistir = document.getElementById("BotaoPersistir");
const BotaoCancelar = document.getElementById("BotaoCancelar");

BotaoSelecionar.addEventListener('click', () => {
    const SelecaoEnderecos = document.getElementsByClassName("SelecaoEnderecos")[0];
    const DivBotoes = document.getElementsByClassName("DivBotoes")[0];

    if (radioIncluir.checked) {
        if (TipoEstoque.value !== "" && Natureza.value !== "") {
            Natureza.disabled = true;
            TipoEstoque.disabled = true;
            radioIncluir.disabled = true;
            radioExcluir.disabled = true;
            DivBotoes.style.display = "flex";
            BotaoPersistir.textContent = "CADASTRAR"
            SelecaoEnderecos.style.display = "flex";
            TipoEstoque.style.borderColor = "";
            Natureza.style.borderColor = "";
        } else {
            if (TipoEstoque.value === "") {
                TipoEstoque.style.borderColor = "red";
            } else {
                TipoEstoque.style.borderColor = "";
            }
            
            if (Natureza.value === "") {
                Natureza.style.borderColor = "red";
            } else {
                Natureza.style.borderColor = "";
            }
    
            setTimeout(() => {
                TipoEstoque.style.borderColor = "";
                Natureza.style.borderColor = "";
            }, 10000);
        }
    } else if (radioExcluir.checked) {
        if (TipoEstoque.value !== "" && Natureza.value !== "") {
            Natureza.disabled = true;
            TipoEstoque.disabled = true;
            radioIncluir.disabled = true;
            radioExcluir.disabled = true;
            DivBotoes.style.display = "flex";
            BotaoPersistir.textContent = "EXCLUIR"
            SelecaoEnderecos.style.display = "flex";
            TipoEstoque.style.borderColor = "";
            Natureza.style.borderColor = "";
        } else {
            if (TipoEstoque.value === "") {
                TipoEstoque.style.borderColor = "red";
            } else {
                TipoEstoque.style.borderColor = "";
            }
            
            if (Natureza.value === "") {
                Natureza.style.borderColor = "red";
            } else {
                Natureza.style.borderColor = "";
            }
    
            setTimeout(() => {
                TipoEstoque.style.borderColor = "";
                Natureza.style.borderColor = "";
            }, 10000);
        }
    } else {
        alert("Favor selecione a opção Incluir ou Excluir!");
    }
});


const BotaoPersistir1 = document.getElementById("BotaoPersistir");
const SelecaoEnderecos = document.getElementsByClassName("SelecaoEnderecos")[0];
const DivBotoes = document.getElementsByClassName("DivBotoes")[0];
BotaoPersistir1.addEventListener('click', () => {
    const inputsSelecao = document.querySelectorAll(".SelecaoEnderecos input[type='text']");
   
    let inputsVazias = false;

    inputsSelecao.forEach(input => {
        if (input.value.trim() === "") {
            input.style.borderColor = "red";
            inputsVazias = true;

            // Define um timeout para remover a borda vermelha após 10 segundos
            setTimeout(() => {
                input.style.borderColor = "";
            }, 10000); // 10000 milissegundos = 10 segundos
        } else {
            input.style.borderColor = ""; // Limpa a borda se o campo não estiver vazio
        }
    });

    if (inputsVazias) {
        return; // Retorna se houver campos vazios
    }

    
    if (radioIncluir.checked) {
        document.getElementById('Imprimir').style.display = 'block';

    } else if (radioExcluir.checked) {
        if (Empresa === "1") {
            CadastrarEnderecos(CadastrarEnderecosMatriz,"DELETE", false);
        } else if (Empresa === "4") {
            CadastrarEnderecos(CadastrarEnderecosFilial,"DELETE", false);
        }
        
        document.getElementById('Imprimir').style.display = 'none'
        
        const inputsSelecao = document.querySelectorAll(".SelecaoEnderecos input[type='text']");
        inputsSelecao.forEach(input => {
            input.value = "";
            Natureza.value = ""; // Limpa a seleção da combobox Natureza
            TipoEstoque.value = ""; // Limpa a seleção da combobox TipoEstoque
            radioIncluir.checked = false; // Desmarca a rádio Incluir
            radioExcluir.checked = false; // Desmarca a rádio Excluir

            Natureza.disabled = false;
            TipoEstoque.disabled = false;
            radioIncluir.disabled = false;
            radioExcluir.disabled = false;
            DivBotoes.style.display = "none";
            BotaoPersistir.textContent = "";
            SelecaoEnderecos.style.display = "none";
            TipoEstoque.style.borderColor = "";
            Natureza.style.borderColor = "";
    })
    }
   
    });

    document.getElementById('confirmButtonImpressao').addEventListener('click', () => {
            if (Empresa === "1") {
                CadastrarEnderecos(CadastrarEnderecosMatriz,"PUT", true);
            } else if (Empresa === "4") {
                CadastrarEnderecos(CadastrarEnderecosFilial,"PUT", true);
            }
            document.getElementById('Imprimir').style.display = 'none'

        const inputsSelecao = document.querySelectorAll(".SelecaoEnderecos input[type='text']");
        inputsSelecao.forEach(input => {
            input.value = "";
            Natureza.value = ""; // Limpa a seleção da combobox Natureza
            TipoEstoque.value = ""; // Limpa a seleção da combobox TipoEstoque
            radioIncluir.checked = false; // Desmarca a rádio Incluir
            radioExcluir.checked = false; // Desmarca a rádio Excluir

            Natureza.disabled = false;
            TipoEstoque.disabled = false;
            radioIncluir.disabled = false;
            radioExcluir.disabled = false;
            DivBotoes.style.display = "none";
            BotaoPersistir.textContent = "";
            SelecaoEnderecos.style.display = "none";
            TipoEstoque.style.borderColor = "";
            Natureza.style.borderColor = "";
    })

})
    

    document.getElementById('cancelButtonImpressao').addEventListener('click', () => {
        
        if (Empresa === "1") {
            CadastrarEnderecos(CadastrarEnderecosMatriz,"PUT", false);
        } else if (Empresa === "4") {
            CadastrarEnderecos(CadastrarEnderecosFilial,"PUT", false);
        }
            document.getElementById('Imprimir').style.display = 'none'
        const inputsSelecao = document.querySelectorAll(".SelecaoEnderecos input[type='text']");
        inputsSelecao.forEach(input => {
            input.value = "";
            Natureza.value = ""; // Limpa a seleção da combobox Natureza
            TipoEstoque.value = ""; // Limpa a seleção da combobox TipoEstoque
            radioIncluir.checked = false; // Desmarca a rádio Incluir
            radioExcluir.checked = false; // Desmarca a rádio Excluir
        
            Natureza.disabled = false;
            TipoEstoque.disabled = false;
            radioIncluir.disabled = false;
            radioExcluir.disabled = false;
            DivBotoes.style.display = "none";
            BotaoPersistir.textContent = "";
            SelecaoEnderecos.style.display = "none";
            TipoEstoque.style.borderColor = "";
            Natureza.style.borderColor = "";
    });

    
    })


    document.getElementById('BotaoCancelar').addEventListener('click', () =>{
        const inputsSelecao = document.querySelectorAll(".SelecaoEnderecos input[type='text']");
        inputsSelecao.forEach(input => {
            input.value = "";
            Natureza.value = ""; // Limpa a seleção da combobox Natureza
            TipoEstoque.value = ""; // Limpa a seleção da combobox TipoEstoque
            radioIncluir.checked = false; // Desmarca a rádio Incluir
            radioExcluir.checked = false; // Desmarca a rádio Excluir
        
            Natureza.disabled = false;
            TipoEstoque.disabled = false;
            radioIncluir.disabled = false;
            radioExcluir.disabled = false;
            DivBotoes.style.display = "none";
            BotaoPersistir.textContent = "";
            SelecaoEnderecos.style.display = "none";
            TipoEstoque.style.borderColor = "";
            Natureza.style.borderColor = "";
        });
    })


   

    window.addEventListener('load', async () => {

        const VerificaLogin = localStorage.getItem('Login');

        if (VerificaLogin !== "Logado") {
            // Se não houver token, redirecione para a página de login
            window.location.href = '/Login_Teste';
        }
    });

    const linkSair = document.querySelector('.right-menu-item li a[href="/Login_Teste"]');

linkSair.addEventListener("click" , async () => {
  localStorage.clear();
});

