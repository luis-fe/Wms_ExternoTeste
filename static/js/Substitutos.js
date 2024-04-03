const Empresa = localStorage.getItem('CodEmpresa');

const rootElement = document.documentElement;

if (Empresa === "1") {
    rootElement.classList.add('palheta-empresa-a');
} else if (Empresa === "4") {
    rootElement.classList.add('palheta-empresa-b');
} else {
    window.location.href = 'Login_Teste';
}

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
}

const ApiDadosSubstitutos = "http://192.168.0.183:5000/api/SubstitutosPorOP";
const ApiDadosCategorias = "http://192.168.0.183:5000/api/CategoriasSubstitutos";
Token = 'a40016aabcx9';

async function ObterSubstitutos() {
    try {
        const response = await fetch(ApiDadosSubstitutos, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': Token
            },
        });

        if (response.ok) {
            const data = await response.json();
            criarTabelaSubstitutos(data);
        } else {
            throw new Error('Erro no retorno');
        }
    } catch (error) {
        console.error(error.message);
    }
}

function criarTabelaSubstitutos(listaSubstitutos) {
    // Remova os elementos paginadores antigos
    $('#PaginacaoUsuarios .dataTables_paginate').remove();

    // Destruir a tabela existente, se houver
    if ($.fn.DataTable.isDataTable('#TabelaSubstitutos')) {
        $('#TabelaSubstitutos').DataTable().destroy();
    }

    // Crie a tabela    
    tabela = $('#TabelaSubstitutos').DataTable({
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
            lengthMenu: 'Mostrar _MENU_ itens por página',
        }
    });

    tabela.clear().rows.add(listaSubstitutos).draw();

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
}

// Adiciona evento de clique ao checkbox "Selecionar Tudo"
$('#selectAllCheckbox').on('change', function () {
    $('.rowCheckbox').prop('checked', $(this).prop('checked'));
});

// Adiciona evento de clique aos checkboxes das linhas
$(document).on('change', '.rowCheckbox', function () {
    $('#selectAllCheckbox').prop('checked', $('.rowCheckbox:checked').length === $('.rowCheckbox').length);
});


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
            selectAllCheckbox.addEventListener('change', function() {
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
                            checkbox.addEventListener('change', function() {
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
            }
            
            // Função para atualizar a tabela com base nas opções selecionadas
            function atualizarTabela() {
                const selectedCategories = [];
                const checkboxes = document.querySelectorAll('.categoriaCheckbox:checked');
                checkboxes.forEach(checkbox => {
                    selectedCategories.push(checkbox.value);
                });
            
                // Aplica o filtro na tabela usando as categorias selecionadas (coluna 7)
                tabela.column(8).search(selectedCategories.join('|'), true, false).draw();
            };


            document.getElementById('ButtonCheck').addEventListener('click', async function() {
                // Arrays para armazenar os dados das linhas selecionadas
                const arrayOP = [];
                const arraycor = [];
                const arraydesconsidera = [];
                
                // Percorre todos os checkboxes das linhas
                $('.rowCheckbox').each(function() {
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
                        ObterSubstitutos();
                        
                    } else {
                        console.error('Erro ao enviar dados para a API:', response.status);
                    }
                } catch (error) {
                    console.error('Erro ao enviar dados para a API:', error);
                }
            }
            
         

            window.addEventListener('load', async ()  => {
                const NomeUsuario = localStorage.getItem('nomeUsuario');
                const VerificaLogin = localStorage.getItem('Login');
                const linkUsuario = document.querySelector('.right-menu-item a');
                if (Empresa === "1") {
                    if (VerificaLogin !== "Logado") {
            
                        window.location.href = 'Login_Teste';
                    } else {
                        await ObterSubstitutos();
                        FuncaoConsultas(ApiDadosCategorias, 'categoria');
                    }
                } else if (Empresa === "4") {
                    if (VerificaLogin !== "Logado") {
                        window.location.href = 'Login_Teste';
                    } else {
                        await ObterSubstitutos();
                        FuncaoConsultas(ApiDadosCategorias, 'categoria');
            
                    }
                }
            });   
            
            const linkSair = document.querySelector('.right-menu-item li a[href="Login_Teste"]');

            linkSair.addEventListener("click" , async () => {
              localStorage.clear();
            });