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
            <h3>FILA DE REPOSIÇÃO</h3>
            <i class="bi bi-x-square-fill" title="Fechar" id="FecharRotina" style="color: black"></i>
        </div>

        <div class="Corpo">
            <div class="tabela">
                <table id="Tabela" class="Tabela">
                    <thead>
                        <tr>
                            <th>Cód. Engenharia</th>
                            <th>Cód. Reduzido</th>
                            <th>Necessidade em Peças</th>
                            <th>Saldo em Fila</th>
                            <th>Op's</th>
                            <th>Qtd. Pedidos Faltando</th>
                            <th>Cód Epc Referencial</th>
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
        const ApiFilaMatriz = 'http://192.168.0.183:5000/api/NecessidadeReposicao';
        const ApiFilaFilial = 'http://192.168.0.184:5000/api/NecessidadeReposicao'
        var empresa = '<?php echo $empresa; ?>';
        var usuario = '<?php echo $usuario; ?>';
        const Token = 'a40016aabcx9';



        function mostrarModalLoading() {
            document.getElementById("modalLoading").style.display = "block";
        }


        function ocultarModalLoading() {
            document.getElementById("modalLoading").style.display = "none";
        }

        async function obterFilaReposicao(API) {
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
                    CriarFilaReposicao(data[0]['1- Detalhamento das Necessidades ']);
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

        function CriarFilaReposicao(ListaFilaReposicao) {
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
                autoWidth: false, // Desativa a largura automática das colunas
                lengthChange: false, // Remove a opção de alterar o número de itens por página
                pageLength: 25,
                columns: [
                    { data: 'engenharia', width: '200px' },
                    { data: 'codreduzido', width: '200px' },
                    { data: 'Necessidade p/repor', width: '150px' },
                    { data: 'saldofila', width: '150px'},
                    { data: 'ops', width: '300px'},
                    { data: 'Qtd_Pedidos que usam', width: '150px' },
                    { data: 'epc_referencial' }
                ],
                language: {
                    paginate: {
                        first: 'Primeira',
                        previous: 'Anterior',
                        next: 'Próxima',
                        last: 'Última',
                    },
                }
            });

            tabela.clear().rows.add(ListaFilaReposicao).draw();

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



        window.addEventListener('load', async () => {
            if (empresa === "1") {

                await obterFilaReposicao(ApiFilaMatriz);

            } else if (empresa === "4") {
                await obterFilaReposicao(ApiFilaFilial);
            }
        });  
    </script>
</body>

</html>