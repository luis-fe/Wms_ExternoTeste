<?php
include_once("./templates/cabecalho.php");
?>

<!DOCTYPE html>
<html lang="pt-BR">

<head>
    <link rel="stylesheet" href="./css/Wms.css">
    <link rel="stylesheet" href="./css/Modais.css">
    <link rel="stylesheet" href="./css/Substitutos.css">
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
            <h3>ANÁLISE DE SUBSTITUTOS</h3>
            <button style="padding: 10px; background-color: black; color: white; height: 40px; cursor: pointer"
                id="ButtonCheck">Selecionar</button>
            <i class="bi bi-x-square-fill" title="Fechar" id="FecharRotina" style="color: black"></i>
        </div>

        <div class="Corpo">
            <div class="Categorias">
                <div class="MenuCategorias1">
                    <div class="MenuCategorias" id="MenuCategorias">
                        <label for="text">Categorias:</label>
                        <i class="bi bi-chevron-down"></i>
                    </div>
                    <div id="CategoriaContainer" class="custom-select-Categoria">
                        <div id="optionsContainer" style="display: block;  ">
                            <label>
                                <input type="checkbox" id="selectAllCategorias" class="selectAll"> Selecionar Tudo
                            </label>
                        </div>
                    </div>
                </div>
            </div>
            <div class="tabela">
                <table id="Tabela" class="Tabela">
                    <thead>
                        <tr>
                            <th><input type="checkbox" id="selectAllCheckbox"></th>
                            <th>Número OP</th>
                            <th>Código Produto</th>
                            <th>Cor Produto</th>
                            <th>Código Principal</th>
                            <th>Descrição Principal</th>
                            <th>Código Substituto</th>
                            <th>Descrição Substituto</th>
                            <th>Categorias</th>
                            <th>Aplicação</th>
                            <th>OP Especial?</th>
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

    <div class="modalLoading" id="modalLoading">
        <div class="modalLoading-content">
            <div class="loader"></div>
            <p>Aguarde, carregando...</p>
        </div>
    </div>

    <script>
        const ApiDadosSubstitutos = "http://192.168.0.183:5000/api/SubstitutosPorOP";
        const ApiDadosCategorias = "http://192.168.0.183:5000/api/CategoriasSubstitutos";
        var empresa = '<?php echo $empresa; ?>';
        var usuario = '<?php echo $usuario; ?>';
        const Token = 'a40016aabcx9';



        function mostrarModalLoading() {
            document.getElementById("modalLoading").style.display = "block";
        }


        function ocultarModalLoading() {
            document.getElementById("modalLoading").style.display = "none";
        }

        async function ObterSubstitutos(API) {
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
                    criarTabelaSubstitutos(data);
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

        function criarTabelaSubstitutos(ListaSubstitutos) {
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
                    {
                        data: null,
                        render: function (data, type, row, meta) {
                            // Cria um checkbox com ID único para cada linha
                            return '<input type="checkbox" class="rowCheckbox" id="checkbox_' + meta.row + '">';
                        }
                    },
                    { data: '2-numeroOP' },
                    { data: '3-codProduto' },
                    { data: '4-cor' },
                    { data: '6-codigoPrinc' },
                    { data: '7-nomePrinc' },
                    { data: '8-codigoSub' },
                    { data: '9-nomeSubst' },
                    { data: '1-categoria' },
                    { data: '10-aplicacao' },
                    { data: 'considera' },
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

            tabela.clear().rows.add(ListaSubstitutos).draw();

            $('.dataTables_paginate').appendTo('#PaginacaoUsuarios');

            $('#PaginacaoUsuarios .paginate_button.previous').on('click', function () {
                tabela.page('previous').draw('page');
            });

            $('#PaginacaoUsuarios .paginate_button.next').on('click', function () {
                tabela.page('next').draw('page');
            });
            // Adiciona evento de clique ao checkbox "Selecionar Tudo"
            $('#selectAllCheckbox').on('change', function () {
                $('.rowCheckbox').prop('checked', $(this).prop('checked'));
            });
            $(document).on('change', '.rowCheckbox', function () {
                $('#selectAllCheckbox').prop('checked', $('.rowCheckbox:checked').length === $('.rowCheckbox').length);
            });

            const paginaInicial = 1;
            tabela.page(paginaInicial - 1).draw('page');

            $('#PaginacaoUsuarios .paginate_button').on('click', function () {
                $('#PaginacaoUsuarios .paginate_button').removeClass('current');
                $(this).addClass('current');
            });
        };

        async function FuncaoConsultas(apiUrl, parametroResultado) {
            const Container1 = document.getElementById('CategoriaContainer');

            try {
                const response = await fetch(apiUrl, {
                    method: 'GET',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': Token
                    },
                });

                if (response.ok) {
                    const data = await response.json();

                    Container1.innerHTML = ''; // Limpa o conteúdo anterior

                    // Adiciona as opções com base nos Dados da API
                    data.forEach(item => {
                        const label = document.createElement('label');
                        const checkbox = document.createElement('input');
                        checkbox.type = 'checkbox';
                        checkbox.className = 'categoriaCheckbox'; // Adiciona a classe para referenciar os checkboxes
                        checkbox.value = item[parametroResultado];
                        label.appendChild(checkbox);
                        label.appendChild(document.createTextNode(item[parametroResultado]));
                        Container1.appendChild(label);
                    });

                    // Adiciona evento de clique ao checkbox "Selecionar Tudo"
                    const selectAllCheckbox = document.createElement('input');
                    selectAllCheckbox.type = 'checkbox';
                    selectAllCheckbox.id = 'selectAll';
                    selectAllCheckbox.addEventListener('change', function () {
                        const checkboxes = document.querySelectorAll('.categoriaCheckbox');
                        checkboxes.forEach(checkbox => {
                            checkbox.checked = selectAllCheckbox.checked;
                        });
                        // Atualiza a tabela ao mudar a seleção
                        atualizarTabela();
                    });
                    Container1.insertBefore(selectAllCheckbox, Container1.firstChild);
                    Container1.insertBefore(document.createTextNode('Selecionar Tudo'), Container1.firstChild);

                    // Adiciona eventos de clique aos checkboxes
                    const checkboxes = document.querySelectorAll('.categoriaCheckbox');
                    checkboxes.forEach(checkbox => {
                        checkbox.addEventListener('change', function () {
                            // Atualiza a tabela ao mudar a seleção
                            atualizarTabela();
                        });
                    });

                } else {
                    throw new Error('Erro no retorno da API');
                }
            } catch (error) {
                console.error(error);
            }
        };

        function atualizarTabela() {
            const selectedCategories = [];
            const checkboxes = document.querySelectorAll('.categoriaCheckbox:checked');
            checkboxes.forEach(checkbox => {
                selectedCategories.push(checkbox.value);
            });

            // Aplica o filtro na tabela usando as categorias selecionadas (coluna 7)
            tabela.column(8).search(selectedCategories.join('|'), true, false).draw();
        };

        window.addEventListener('load', async () => {
            if (empresa === "1") {

                await ObterSubstitutos(ApiDadosSubstitutos);
                FuncaoConsultas(ApiDadosCategorias, 'categoria')

            } else if (empresa === "4") {
                //
            }
        });


        const containers = {
            Categorias: {
                filtro: document.getElementById('MenuCategorias'),
                tela: document.getElementById('CategoriaContainer'),
                selectAllCheckbox: document.getElementById('selectAllCategorias'),
                classeCheckbox: 'custom-select-Categoria'
            },
        };

        let containerAtual = null;

        // Adiciona um ouvinte de eventos ao documento para detectar cliques fora do container
        document.addEventListener('click', (event) => {
            let clicouEmContainer = false;

            // Verifica se o clique está dentro de algum container
            for (const containerKey in containers) {
                const container = containers[containerKey];

                if (container.filtro.contains(event.target) || container.tela.contains(event.target)) {
                    clicouEmContainer = true;
                    break;
                }
            }

            // Fecha o container atual se não clicou em nenhum container
            if (!clicouEmContainer && containerAtual) {
                containerAtual.tela.style.display = 'none';
                containerAtual = null; // Reseta o container atual
            }
        });

        // Itera pelos containers para adicionar ouvintes de eventos
        for (const containerKey in containers) {
            const container = containers[containerKey];

            container.filtro.addEventListener('click', () => {
                // Fecha o container atual se já estiver aberto
                if (containerAtual && containerAtual !== container) {
                    containerAtual.tela.style.display = 'none';
                }
                containerAtual = container;
                container.tela.style.display = container.tela.style.display === 'block' ? 'none' : 'block';
            });
        };

        document.getElementById('ButtonCheck').addEventListener('click', async function () {
            // Arrays para armazenar os dados das linhas selecionadas
            const arrayOP = [];
            const arraycor = [];
            const arraydesconsidera = [];

            // Percorre todos os checkboxes das linhas
            $('.rowCheckbox').each(function () {
                // Verifica se o checkbox está marcado
                if ($(this).is(':checked')) {
                    // Obtém os dados da linha correspondente ao checkbox marcado
                    const row = tabela.row($(this).closest('tr')).data();
                    // Adiciona os campos desejados aos arrays correspondentes
                    arrayOP.push(row['2-numeroOP']);
                    arraycor.push(row['4-cor']);
                    if (row['considera'] === 'sim') {
                        // Se for 'sim', adiciona '-' ao arraydesconsidera
                        arraydesconsidera.push('-');
                    } else {
                        // Se não for 'sim', adiciona 'sim' ao arraydesconsidera
                        arraydesconsidera.push('sim');
                    }
                }
            });

            // Cria o objeto com os arrays
            const dadosSelecionados = {
                arrayOP: arrayOP,
                arraycor: arraycor,
                arraydesconsidera: arraydesconsidera
            };

            // Chama a função para enviar os dados selecionados para a API
            await enviarDadosParaAPI(dadosSelecionados);
        });

        async function enviarDadosParaAPI(dados) {
            try {
                const response = await fetch('http://192.168.0.183:5000/api/SalvarSubstitutos', {
                    method: 'PUT',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': Token,
                    },
                    body: JSON.stringify(dados),
                });

                if (response.ok) {
                    const data = await response.json();
                    ObterSubstitutos(ApiDadosSubstitutos);

                } else {
                    console.error('Erro ao enviar dados para a API:', response.status);
                }
            } catch (error) {
                console.error('Erro ao enviar dados para a API:', error);
            }
        }
    </script>
</body>

</html>