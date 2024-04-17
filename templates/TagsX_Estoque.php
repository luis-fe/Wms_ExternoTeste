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
            <h3>TAG'S x ESTOQUE</h3>
            <i class="bi bi-x-square-fill" title="Fechar" id="FecharRotina" style="color: black"></i>
        </div>

        <div class="Corpo">
            <div class="tabela">
                <table id="Tabela" class="Tabela">
                    <thead>
                        <tr>
                            <th>Cód. Reduzido</th>
                            <th>Tags em Conferência</th>
                            <th>Tags WMS</th>
                            <th>Tags Posição de Estoque</th>
                            <th>Diferença</th>
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
        const ApiComparativoMatriz = "http://192.168.0.183:5000/api/confrontoTags"; 
        const ApiComparativoFilial = "http://192.168.0.184:5000/api/confrontoTags";
        var empresa = '<?php echo $empresa; ?>';
        var usuario = '<?php echo $usuario; ?>';
        const Token = 'a40016aabcx9';



        function mostrarModalLoading() {
            document.getElementById("modalLoading").style.display = "block";
        }


        function ocultarModalLoading() {
            document.getElementById("modalLoading").style.display = "none";
        }

        async function obterRelatorio(API) {
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
                    CriarTabelaRelatorio(data[0]['4- Detalhamento ']);
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

        function CriarTabelaRelatorio(ListaRelatorio) {
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
                    { data: 'reduzido' },
                    { data: 'em_conferencia' },
                    { data: 'situacao3' },
                    { data: 'posicao_estoque' },
                    { data: 'diferenca' }
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

            tabela.clear().rows.add(ListaRelatorio).draw();

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

                await obterRelatorio(ApiComparativoMatriz);

            } else if (empresa === "4") {
                await obterRelatorio(ApiComparativoFilial);
            }
        });  
    </script>
</body>

</html>