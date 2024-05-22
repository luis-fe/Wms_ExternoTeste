<?php
include_once("./templates/cabecalho.php");
?>

<!DOCTYPE html>
<html lang="pt-BR">

<head>
    <link rel="stylesheet" href="./css/ContainerWms.css">
    <link rel="stylesheet" href="./css/Modais.css">
    <style>
        #ImagemPreview {
            padding: auto;
            margin: auto;
            max-width: 150px;
            max-height: 150px;
        }

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
            0% {
                transform: rotate(0deg);
            }

            100% {
                transform: rotate(360deg);
            }
        }
    </style>
</head>

<body class="<?php echo $classe_empresa; ?>">
    <div class="Container">
        <div class="title">
            <h3>GERENCIAMENTO DE CHAMADOS</h3>
            <i class="bi bi-database-fill-add" onclick="AbrirModal('ModalCad')"></i>
            <i class="bi bi-x-square-fill" title="Fechar" id="FecharRotina" style="color: black"></i>
        </div>

        <div class="Corpo">
            <div class="tabela">
                <table id="Tabela" class="Tabela">
                    <thead>
                        <tr>
                            <th>Id Chamado</th>
                            <th>Descrição Chamado</th>
                            <th>Solicitante</th>
                            <th>Data Chamado</th>
                            <th>Data Finalização</th>
                            <th>Tipo de Chamado</th>
                            <th>Status do Chamado</th>
                            <th>Ações</th>
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
            <h2>Novo Chamado</h2>
            <form action="" id="FormCad">
                <div class="Inputs">
                    <textarea id="InputDescricaoChamado" placeholder="Descrição do Chamado" rows="4" required
                        style="height: 150px; width: calc(100%); margin: 10px 0; padding: 12px; border: 1px solid var(--cor2); border-radius: 5px; font-size: 16px; transition: border-color 0.3s ease;"></textarea>
                    <input type="file" id="InputImagem" style="display: none;" accept="image/*">
                    <img id="ImagemPreview" src="#" alt="Imagem Selecionada" style="display: none;">
                    <div class="BotoesImagens" style="display: flex;align-items: center">
                        <i class="bi bi-camera-fill" onclick="document.getElementById('InputImagem').click();"
                            title="Selecionar Imagem" style="font-size: 30px; cursor: pointer"></i>
                        <i class="bi bi-trash-fill" id="DeleteImagem" onclick="LimparImagem();" title="Excluir Imagem"
                            style="font-size: 25px; cursor: pointer; display: none"></i>
                    </div>
                </div>
                <div class="Inputs">
                    <select id="TipoChamado" required>
                        <option disabled selected value="">Tipo de Chamado</option>
                        <option value="MANUTENÇÃO">MANUTENÇÃO</option>
                        <option value="MELHORIA">MELHORIA</option>
                    </select>
                </div>
                <div class="Inputs" required>
                    <select id="AreaChamado">
                        <option disabled selected value="">Área de Chamado</option>
                    </select>
                </div>
                <div class="buttonedit">
                    <button type="submit" id="SalvarEditUser">Salvar</button>
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

    <script>
        const ApiConsultaChamadosMatriz = "http://192.168.0.183:5000/api/chamados";
        const ApiConsultaChamadosFilial = "http://192.168.0.184:5000/api/chamados";
        const ApiNovoChamadoMatriz = "http://192.168.0.183:5000/api/NovoChamado";
        const ApiNovoChamadoFilial = "http://192.168.0.184:5000/api/NovoChamado";
        const ApiImagemMatriz = "http://192.168.0.183:5000/api/upload";
        const ApiImagemFilial = "http://192.168.0.184:5000/api/upload";
        const ApiConsultaAreaChamadosMatriz = "http://192.168.0.183:5000/api/area";
        const ApiConsultaAreaChamadosFilial = "http://192.168.0.184:5000/api/area";
        var empresa = '<?php echo $empresa; ?>';
        var usuario = '<?php echo $usuario; ?>';
        const Token = 'a40016aabcx9';
        const ModalCad = document.getElementById('ModalCad');
        var IdChamado = "";
        var imagemSelecionada = null;

        document.getElementById('InputImagem').addEventListener('change', function (event) {
            const file = event.target.files[0];

            if (file) {
                const reader = new FileReader();

                reader.onload = function (e) {
                    imagemSelecionada = e.target.result;
                    document.getElementById('ImagemPreview').src = imagemSelecionada;
                    document.getElementById('ImagemPreview').style.display = 'block';
                };

                reader.readAsDataURL(file);
            }
            document.getElementById('DeleteImagem').style.display = "flex"
        });

        function LimparImagem() {
            imagemSelecionada = null;
            document.getElementById('InputImagem').value = ''; // Limpa o input file
            document.getElementById('ImagemPreview').src = ''; // Limpa a imagem do preview
            document.getElementById('ImagemPreview').style.display = 'none'; // Esconde o preview da imagem
            document.getElementById('DeleteImagem').style.display = 'none'; // Esconde o preview da imagem
        }

        function AbrirModal(Modal) {
            document.getElementById(Modal).style.display = 'block';
        };

        function FecharModal(Modal) {
            document.getElementById(Modal).style.display = 'none';
            document.getElementById('InputDescricaoChamado').value = "";
            document.getElementById('TipoChamado').value = "";
            document.getElementById('AreaChamado').value = "";
            LimparImagem();
        }

        function mostrarModalLoading() {
            document.getElementById("modalLoading").style.display = "block";
        }


        function ocultarModalLoading() {
            document.getElementById("modalLoading").style.display = "none";
        }

        async function ObterChamados(API) {
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
                    CriarTabelaChamados(data);
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

        async function obterArea(API) {
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
                    const selectAreaChamado = document.getElementById('AreaChamado');
                    console.log(data)
                    selectAreaChamado.innerHTML = ''; // Limpa todas as opções existentes

                    const defaultOption = document.createElement('option');
                    defaultOption.disabled = true;
                    defaultOption.selected = true;
                    defaultOption.value = '';
                    defaultOption.textContent = 'Área de Chamado';
                    selectAreaChamado.appendChild(defaultOption);

                    // Adiciona as novas opções baseadas nos dados recebidos da API
                    data.forEach(item => {
                        const option = document.createElement('option');
                        option.value = item.area; // Define o valor da opção como o campo 'area'
                        option.textContent = item.area; // Define o texto da opção como o campo 'area'
                        selectAreaChamado.appendChild(option);
                    });

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


        function CriarTabelaChamados(listaChamados) {
            $('#PaginacaoUsuarios .dataTables_paginate').remove();

            if ($.fn.DataTable.isDataTable('#Tabela')) {
                $('#Tabela').DataTable().destroy();
            }

            tabela = $('#Tabela').DataTable({
                paging: true,
                info: false,
                searching: true,
                colReorder: true,
                colResize: true,
                columns: [
                    { data: 'id_chamado' },
                    { data: 'descricao_chamado' },
                    { data: 'solicitante' },
                    { data: 'data_chamado' },
                    { data: 'data_finalizacao_chamado' },
                    { data: 'tipo_chamado' },
                    { data: 'status_chamado' },
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

            tabela.clear().rows.add(listaChamados).draw();

            $('.dataTables_paginate').appendTo('#PaginacaoUsuarios');

            $('#PaginacaoUsuarios .paginate_button.previous').on('click', function () {
                tabela.page('previous').draw('page');
            });

            $('#PaginacaoUsuarios .paginate_button.next').on('click', function () {
                tabela.page('next').draw('page');
            });

            const paginaInicial = 1;
            tabela.page(paginaInicial - 1).draw('page');

            $('#PaginacaoUsuarios .paginate_button').on('click', function () {
                $('#PaginacaoUsuarios .paginate_button').removeClass('current');
                $(this).addClass('current');
            });
        };

        async function SalvarChamados(api) {
            const dataAtual = new Date();
            const ano = dataAtual.getFullYear().toString();
            const mes = (dataAtual.getMonth() + 1).toString().padStart(2, '0'); // +1 porque os meses são baseados em zero
            const dia = dataAtual.getDate().toString().padStart(2, '0');
            const dataFormatada = `${ano}/${mes}/${dia}`;
            const Area = document.getElementById('AreaChamado');
            console.log(Area.value);
            console.log(Area.textContent);

            console.log(usuario)
            const Salvar = {
                "solicitante": usuario,
                "data_chamado": dataFormatada,
                "tipo_chamado": document.getElementById('TipoChamado').value,
                "area": document.getElementById('AreaChamado').value,
                "descricao_chamado": document.getElementById('InputDescricaoChamado').value
            };

            console.log(Salvar)
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
                    const resultado = data["id_chamado"]
                    FecharModal('ModalCad');
                    Swal.fire({
                        title: "Chamado Adicionado",
                        icon: "success",
                        showConfirmButton: false,
                        timer: "3000",
                    });
                    IdChamado = resultado;
                    console.log(IdChamado)
                    if (empresa === "1") {
                        ObterChamados(ApiConsultaChamadosMatriz);
                        obterArea(ApiConsultaAreaChamadosMatriz);
                    } else if (empresa === "4") {
                        ObterChamados(ApiConsultaChamadosFilial);
                        obterArea(ApiConsultaAreaChamadosFilial);
                    }

                } else {
                    throw new Error('Erro No Retorno');
                    FecharModal('ModalCad');
                    Swal.fire({
                        title: "Erro",
                        icon: "error",
                        showConfirmButton: false,
                        timer: "3000",
                    });
                    if (empresa === "1") {
                        ObterChamados(ApiConsultaChamadosMatriz);
                        obterArea(ApiConsultaAreaChamadosMatriz);
                    } else if (empresa === "4") {
                        ObterChamados(ApiConsultaChamadosFilial);
                        obterArea(ApiConsultaAreaChamadosFilial);
                    }

                }
            } catch (error) {
                console.error(error);
                FecharModal('ModalCad');
                Swal.fire({
                    title: "Erro",
                    icon: "error",
                    showConfirmButton: false,
                    timer: "3000",
                });
                if (empresa === "1") {
                    ObterChamados(ApiConsultaChamadosMatriz);
                    obterArea(ApiConsultaAreaChamadosMatriz);
                } else if (empresa === "4") {
                    ObterChamados(ApiConsultaChamadosFilial);
                    obterArea(ApiConsultaAreaChamadosFilial);
                }

            }
        };

        async function EnviarImagemParaAPI(api) {
            // Obtenha o elemento de input do arquivo
            const fileInput = document.getElementById("InputImagem");

            // Verifique se um arquivo foi selecionado
            if (fileInput.files.length > 0) {
                const formData = new FormData();
                formData.append('file', fileInput.files[0]); // 'file' deve corresponder à chave esperada pelo backend


                try {
                    const response = await fetch(`${api}/${IdChamado}`, {
                        method: 'POST',
                        headers: {
                            'Authorization': Token,
                        },
                        body: formData,
                    });

                    if (response.ok) {
                        const data = await response.json();
                        console.log('Imagem enviada com sucesso:', data);
                    } else {
                        console.error('Erro ao enviar imagem para a API.');
                    }
                } catch (error) {
                    console.error(error);
                }
            } else {
                console.error('Nenhuma imagem selecionada.');
            }
        }


        document.getElementById('FormCad').addEventListener('submit', async function (event) {
            // Prevenir o envio padrão do formulário
            event.preventDefault();

            if (empresa === '1') {
                await SalvarChamados(ApiNovoChamadoMatriz);
                await EnviarImagemParaAPI(ApiImagemMatriz);
                obterArea(ApiConsultaAreaChamadosMatriz);
            } else if (empresa === '4') {
                SalvarChamados(ApiNovoChamadoFilial);
                await EnviarImagemParaAPI(ApiImagemFilial);
                obterArea(ApiConsultaAreaChamadosFilial);
            }
        });

        window.addEventListener('load', async () => {
            if (empresa === "1") {
                await ObterChamados(ApiConsultaChamadosMatriz);
                await obterArea(ApiConsultaAreaChamadosMatriz); // Chama obterArea para preencher as opções da lista suspensa
            } else if (empresa === "4") {
                await ObterChamados(ApiConsultaChamadosFilial);
                await obterArea(ApiConsultaAreaChamadosFilial); // Chama obterArea para preencher as opções da lista suspensa
            }
        });

    </script>
</body>

</html>