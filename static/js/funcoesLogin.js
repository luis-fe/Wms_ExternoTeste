const UsuarioLogin = document.getElementById("InputLogin");
const SenhaLogin = document.getElementById("InputSenha");
const EmpresaLogin = document.getElementById("InputEmpresa");
const UsuarioNegado = document.getElementById("UsuarioNegado");
const KeyApi = "a40016aabcx9";

function Login() {
    const codigo = UsuarioLogin.value;
    const senha = SenhaLogin.value; 
    const Empresa = EmpresaLogin.value;

    fetch(`http://192.168.0.183:5000/api/UsuarioSenha?codigo=${codigo}&senha=${senha}`, {
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
            throw new Error('Erro ao fazer login');
        }
    })
    .then(data => {
        console.log(data);
        if(data.status === true && data.empresa === Empresa && data.funcao === "ADMINISTRADOR") {
            const NomeUsuario = data.nome;
            localStorage.setItem('nomeUsuario', NomeUsuario);
            localStorage.setItem('CodEmpresa', Empresa);
            window.location.href = "index.html";
            return
        } 
        if(data.empresa !== Empresa) {
            UsuarioNegado.textContent = "Empresa não confere com o Usuário!"
            return
        }
        if(data.funcao !== "ADMINISTRADOR") {
            UsuarioNegado.textContent = "Usuário não possui acesso de Administrador!"
            return
        }

        if(data.funcao !== "ADMINISTRADOR") {
            UsuarioNegado.textContent = "Usuário não possui acesso de Administrador!"
            return
        }

        if(data.status === false) {
            UsuarioNegado.textContent = "Usuário e Senha não conferem!"
            return
        }

    })
    .catch(error => {
        console.error(error);

    });
}

