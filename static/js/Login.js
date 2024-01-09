const UsuarioLogin = document.getElementById("InputLogin");
const SenhaLogin = document.getElementById("InputSenha");
const EmpresaLogin = document.getElementById("InputEmpresa");
const UsuarioNegado = document.getElementById("UsuarioNegado");
const KeyApi = "a40016aabcx9";
const ApiValidacaoMatriz = "http://192.168.0.183:5000/api/UsuarioSenha";
const ApiValidacaoFilial = "http://177.221.240.74:5000/api/UsuarioSenha";

function ValidacaoEmpresa() {
    const EmpresaSelecionada = EmpresaLogin.value;
    console.log(EmpresaSelecionada);

    if (EmpresaSelecionada === "1") {
        Login(ApiValidacaoMatriz);
    } else if (EmpresaSelecionada === "4") {
        Login(ApiValidacaoFilial);
    } else {
        console.error("Empresa não reconhecida!");
        return;
    }
}

function Login(api) {
    const codigo = UsuarioLogin.value;
    const senha = SenhaLogin.value;
    const Empresa = EmpresaLogin.value;

    fetch(`${api}?codigo=${codigo}&senha=${senha}`, {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': KeyApi
        },
    })
    .then(response => {
        if (response.ok) {
            return response.json();
        } else {
            UsuarioNegado.textContent = "Usuário ou Senha não Conferem!";
            throw new Error('Erro na autenticação');
        }
    })
    .then(data => {
        console.log(data);
        if (data.status === true && data.empresa === Empresa && data.funcao === "ADMINISTRADOR") {
            const NomeUsuario = data.nome;
            localStorage.setItem('nomeUsuario', NomeUsuario);
            localStorage.setItem('CodEmpresa', Empresa);
            localStorage.setItem('Login', "Logado");
            window.location.href = "indexMatriz.html";
        } else {
            UsuarioNegado.textContent = "Informações de login inválidas!";
        }
    })
    .catch(error => {
        console.error(error);
        UsuarioNegado.textContent = "Ocorreu um erro durante o login.";
    });
}
