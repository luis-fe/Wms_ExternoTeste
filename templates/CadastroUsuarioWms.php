<?php
include_once("./templates/cabecalho.php");

?>

<!DOCTYPE html>
<html lang="pt-BR">

<head>
    <link rel="stylesheet" href="./css/ContainerWms.css">
    <link rel="stylesheet" href="./css/Modais.css">

</head>
<style>
    .modalLoading {
        display: none;
        position: fixed;
        z-index: 9999;
        left: 0;
        top: 0;
        width: 100%;
        height: 100%;
        background-color: white;
    }
    
    
    .modalLoading-content {
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        background-color: white;
        padding: 20px;
        text-align: center;
        border-radius: 8px;
      }
      
      .loader {
        border: 4px solid #f3f3f3;
        border-radius: 50%;
        border-top: 4px solid var(--cor2);
        width: 40px;
        height: 40px;
        animation: spin 2s linear infinite;
        margin: 0 auto 20px auto;
      }
      
      @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
      }
</style>

<body class="<?php echo $classe_empresa; ?>">
    <div class="Container">
        <div class="title">
            <h3>GERENCIAMENTO DE USUÁRIOS</h3>
            <i class="bi bi-person-fill-add" onclick="AbrirModal('ModalCad')"></i>
            <i class="bi bi-x-square-fill" title="Fechar" id="FecharRotina" style="color: black"></i>
        </div>

        <div class="Corpo">
            <div class="tabela">
                <table id="Tabela" class="Tabela">
                    <thead>
                        <tr>
                            <th>Código do Usuário</th>
                            <th>Nome do Usuário</th>
                            <th>Função</th>
                            <th>Situação</th>
                            <th>Editar</th>
                        </tr>
                    </thead>
                    <tbody id="tBodyTabela">
                    </tbody>
                </table>
            </div>
            <div class="botoes" style="display: flex; margin: 20px auto; width: 80%; justify-content: space-around;">
                <div id="PaginacaoUsuarios" class="dataTables_paginate" style="width: 100%;"></div>
            </div>
        </div>
    </div>


    <div class="Modal" id="ModalCad">
        <div class="Modal-Content">
            <span class="Close-Modal" onclick="FecharModal('ModalCad')">&times;</span>
            <h2>Novo Usuário</h2>
            <form action="" id="FormCad">
                <div class="Inputs">
                    <input type="text" id="InputCadMatricula" required placeholder="Matrícula do Usuário"
                        autocomplete="off">
                </div>
                <div class="Inputs">
                    <input type="text" id="InputCadNome" required placeholder="Nome do Usuário" autocomplete="off">
                </div>
                <div class="Inputs">
                    <select id="Funcao">
                        <option disabled selected value="">Função do Usuário</option>
                        <option value="ADMINISTRADOR">ADMINISTRADOR</option>
                        <option value="RESPOSITOR">RESPOSITOR</option>
                        <option value="SEPARADOR">SEPARADOR</option>
                    </select>
                </div>
                <div class="Inputs">
                    <select id="Situacao">
                        <option disabled selected value="">Situação do Usuário</option>
                        <option value="ATIVO">ATIVO</option>
                        <option value="INATIVO">INATIVO</option>
                    </select>
                </div>
                <div class="Inputs">
                    <input type="password" id="InputCadSenha" required placeholder="Senha do Usuário"
                        autocomplete="off">
                </div>
                <div class="buttonedit">
                    <button type="submit" id="SalvarEditUser">Salvar</button>
                </div>
            </form>
        </div>
    </div>

    <div class="Modal" id="ModalEdit">
        <div class="Modal-Content">
            <span class="Close-Modal" onclick="FecharModal('ModalEdit')">&times;</span>
            <h2>Editar Usuário</h2>
            <form action="" id="FormEdit">
                <div class="Inputs">
                    <input type="text" id="InputEditMatricula" required placeholder="Matrícula do Usuário" readonly
                        style="background-color: lightgray">
                </div>
                <div class="Inputs">
                    <input type="text" id="InputEditNome" required placeholder="Nome do Usuário" autocomplete="off">
                </div>
                <div class="Inputs">
                    <select id="EditFuncao">
                        <option disabled selected value="">Função do Usuário</option>
                        <option value="ADMINISTRADOR">ADMINISTRADOR</option>
                        <option value="REPOSITOR">REPOSITOR</option>
                        <option value="SEPARADOR">SEPARADOR</option>
                    </select>
                </div>
                <div class="Inputs">
                    <select id="EditSituacao">
                        <option disabled selected value="">Situação do Usuário</option>
                        <option value="ATIVO">ATIVO</option>
                        <option value="INATIVO">INATIVO</option>
                    </select>
                </div>
                <div class="buttonedit">
                    <button type="submit">Salvar</button>
                </div>
            </form>
        </div>
    </div>

    <div class="modalLoading" id="modalLoading">
        <div class="modalLoading-content">
            <div class="loader"></div>
            <p>Aguarde, carregando...</p>
        </div>
    </div>

</body>

<script>
    const ApiUsuariosMatriz = "http://192.168.0.183:5000/api/Usuarios";
    const ApiUsuariosFilial = "http://192.168.0.184:5000/api/Usuarios";
    var empresa = '<?php echo $empresa; ?>';
    const Token = 'a40016aabcx9';
    const ModalCad = document.getElementById('ModalCad');
    const EditMatricula = document.getElementById('InputEditMatricula');
    const EditNome = document.getElementById('InputEditNome');
    const EditSituacao = document.getElementById('EditSituacao');
    const EditFuncao = document.getElementById('EditFuncao');

    console.log(empresa);

    function AbrirModal(Modal) {
        document.getElementById(Modal).style.display = 'block';
    };

    function AbrirModalEdit(Modal, Matricula, Nome, Funcao, Situacao) {
        console.log(Funcao)
        document.getElementById(Modal).style.display = 'block';
        EditMatricula.value = Matricula;
        EditNome.value = Nome;
        EditSituacao.value = Situacao;
        EditFuncao.value = Funcao;

    };

    function FecharModal(Modal) {
        document.getElementById(Modal).style.display = 'none';
    }

    function mostrarModalLoading() {
        document.getElementById("modalLoading").style.display = "block";
    }


    function ocultarModalLoading() {
        document.getElementById("modalLoading").style.display = "none";
    }


    async function obterUsuarios(API) {
        mostrarModalLoading();
        try {
            const response = await fetch(API, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': Token
                },
            });

            if (response.ok) {
                const data = await response.json();
                console.log(data);
                criarTabelaUsuarios(data);
                ocultarModalLoading();
            } else {
                throw new Error('Erro no retorno');
                ocultarModalLoading();
            }
        } catch (error) {
            console.error(error.message);
            ocultarModalLoading();
        }
    }

    function criarTabelaUsuarios(listaUsuarios) {
        // Remova os elementos paginadores antigos
        $('#PaginacaoUsuarios .dataTables_paginate').remove();

        // Destruir a tabela existente, se houver
        if ($.fn.DataTable.isDataTable('#Tabela')) {
            $('#Tabela').DataTable().destroy();
        }

        // Crie a tabela
        tabela = $('#Tabela').DataTable({
            paging: true,
            info: false,
            searching: true,
            colReorder: true,
            colResize: true,
            columns: [
                { data: 'codigo' },
                { data: 'nome' },
                { data: 'situacao' },
                { data: 'funcao' },
                {
                    data: null,
                    render: function (data, type, row) {
                        return `
                        <div class="acoes">
                            <i class="bi bi-pencil btn-editar" title="Editar" onclick="AbrirModalEdit('${'ModalEdit'}','${row.codigo}', '${row.nome}', '${row.funcao}', '${row.situacao}')"></i>
                        </div>
                    `;
                    }
                }
            ],
            language: {
                paginate: {
                    first: 'Primeira',
                    previous: 'Anterior',
                    next: 'Próxima',
                    last: 'Última',
                },
                lengthMenu: 'Mostrar_MENU_Itens Por Página',
            }
        });

        tabela.clear().rows.add(listaUsuarios).draw();

        // Adicionar a div .dataTables_paginate ao final da tabela
        $('.dataTables_paginate').appendTo('#PaginacaoUsuarios');

        // Adicionar eventos de clique aos botões padrão do DataTable
        $('#PaginacaoUsuarios .paginate_button.previous').on('click', function () {
            tabela.page('previous').draw('page');
        });

        $('#PaginacaoUsuarios .paginate_button.next').on('click', function () {
            tabela.page('next').draw('page');
        });

        // Forçar a página inicial
        const paginaInicial = 1;
        tabela.page(paginaInicial - 1).draw('page');

        $('#PaginacaoUsuarios .paginate_button').on('click', function () {
            $('#PaginacaoUsuarios .paginate_button').removeClass('current');
            $(this).addClass('current');
        });
    };


    async function CadUsuario(Api) {
        const CadMatricula = document.getElementById('InputCadMatricula');
        const CadNome = document.getElementById('InputCadNome');
        const CadSenha = document.getElementById('InputCadSenha');
        const CadSituacao = document.getElementById('Situacao');
        const CadFuncao = document.getElementById('Funcao');

        dados = {
            "codigo": CadMatricula.value,
            "nome": CadNome.value,
            "funcao": CadFuncao.value,
            "situacao": CadSituacao.value,
            "senha": CadSenha.value,
        }

        try {
            const response = await fetch(`${Api}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': Token
                },
                body: JSON.stringify(dados),
            });

            if (response.ok) {
                const data = await response.json();
                FecharModal('ModalCad');
                CadMatricula.value = "";
                CadNome.value = "";
                CadSenha.value = "";
                CadSituacao.value = "";
                CadFuncao.value = "";
                Swal.fire({
                    title: "Usuário Cadastrado",
                    icon: "success",
                    showConfirmButton: false,
                    timer: "3000",
                });
                obterUsuarios(Api);
            } else {
                throw new Error('Erro ao obter os dados da API');
                FecharModal('ModalCad');
                CadMatricula.value = "";
                CadNome.value = "";
                CadSenha.value = "";
                CadSituacao.value = "";
                CadFuncao.value = "";
                Swal.fire({
                    title: "Erro",
                    icon: "error",
                    showConfirmButton: false,
                    timer: "3000",
                });
                obterUsuarios(Api);
            }
        } catch (error) {
            console.error(error);
            FecharModal('ModalCad');
                CadMatricula.value = "";
                CadNome.value = "";
                CadSenha.value = "";
                CadSituacao.value = "";
                CadFuncao.value = "";
            Swal.fire({
                title: "Erro",
                icon: "error",
                showConfirmButton: false,
                timer: "3000",
            });
            obterUsuarios(Api);
        }
    };

    async function EditUsuarios(Api) {
        dados = {
            "codigo": EditMatricula.value,
            "nome": EditNome.value,
            "funcao": EditFuncao.value,
            "situacao": EditSituacao.value,
        }
        try {
            const response = await fetch(`${Api}/${EditMatricula.value}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': Token
                },
                body: JSON.stringify(dados),
            });

            if (response.ok) {
                const data = await response.json();
                FecharModal('ModalEdit');
                EditMatricula.value = "";
                EditNome.value = "";
                EditFuncao.value = "";
                EditSituacao.value = "";
                Swal.fire({
                    title: "Usuário Atualizado",
                    icon: "success",
                    showConfirmButton: false,
                    timer: "3000",
                });
                obterUsuarios(Api);
            } else {
                throw new Error('Erro ao obter os dados da API');
                FecharModal('ModalEdit');
                EditMatricula.value = "";
                EditNome.value = "";
                EditFuncao.value = "";
                EditSituacao.value = "";
                Swal.fire({
                    title: "Erro",
                    icon: "error",
                    showConfirmButton: false,
                    timer: "3000",
                });
                obterUsuarios(Api);
            }
        } catch (error) {
            console.error(error);
            FecharModal('ModalEdit');
                EditMatricula.value = "";
                EditNome.value = "";
                EditFuncao.value = "";
                EditSituacao.value = "";
            Swal.fire({
                title: "Erro",
                icon: "error",
                showConfirmButton: false,
                timer: "3000",
            });
            obterUsuarios(Api);
        }
    };



    document.getElementById('FormCad').addEventListener('submit', function (event) {
        // Prevenir o envio padrão do formulário
        event.preventDefault();

        if (empresa === '1') {
            CadUsuario(ApiUsuariosMatriz);
        } else if (empresa === '4') {
            CadUsuario(ApiUsuariosFilial);
        }
    });

    document.getElementById('FormEdit').addEventListener('submit', function (event) {
        // Prevenir o envio padrão do formulário
        event.preventDefault();

        if (empresa === '1') {
            EditUsuarios(ApiUsuariosMatriz);
        } else if (empresa === '4') {
            EditUsuarios(ApiUsuariosFilial);
        }
    });


    window.addEventListener('load', async () => {

        if (empresa === "1") {

            await obterUsuarios(ApiUsuariosMatriz);

        } else if (empresa === "4") {

            await obterUsuarios(ApiUsuariosFilial);
        }
    });   
</script>

</html>