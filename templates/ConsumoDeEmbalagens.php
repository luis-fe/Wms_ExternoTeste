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
    /* Estilos para a modal de loading */
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

      input[type="date"]{
        padding: 3.5px;
        border-radius: 10px 
      }

      button{
        padding: 3.5px;
        background-color: var(--cor2);
        color: white;
        border-radius: 10px;
        width: 200px;
        cursor: pointer
      }

</style>

<body class="<?php echo $classe_empresa; ?>">
    <div class="Container">
        <div class="title">
            <h3>CONSUMO DE EMBALAGENS</h3>
            <i class="bi bi-x-square-fill" title="Fechar" id="FecharRotina" style="color: black"></i>
        </div>
        <div class="Corpo">
            <div class="Datas">
                <label for="text">Data Inicio</label>
                <input type="date" id="DataInicial">
                <label for="text">Data Fim</label>
                <input type="date" id="DataFinal">
                <button id="btnFiltrar">Filtrar</button>
                <button id="btnCadastrar">Cadastrar Embalagem</button>
            </div>
            <div class="tabela">
                <table id="Tabela" class="Tabela">
                    <thead>
                        <tr>
                            <th>Cód. Caixa</th>
                            <th>Tam. Caixa</th>
                            <th>Quantidade</th>
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

    <div class="Modal" id="ModalCadEmbalagem">
        <div class="Modal-Content">
            <span class="Close-Modal" onclick="FecharModal('ModalCadEmbalagem')">&times;</span>
            <h2>Nova Embalagem</h2>
            <form action="" id="FormCadEmbalgem">
                <div class="Inputs">
                    <input type="text" id="InputCodCaixa" required placeholder="Código da Caixa"
                        autocomplete="off">
                </div>
                <div class="Inputs">
                    <input type="text" id="InputDescricao" required placeholder="Descrição da Caixa" autocomplete="off">
                </div>
                <div class="Inputs">
                    <input type="text" id="InputTamanhoCaixa" required placeholder="Tamanho da Caixa"
                        autocomplete="off">
                </div>
                <div class="buttonedit">
                    <button type="submit" id="salvarCaixa">Salvar</button>
                </div>
            </form>
        </div>
    </div>

    <script>
        const apiCadastroMatriz= 'http://192.168.0.183:5000/api/CadastrarCaixa';
        const apiCadastroFilial = 'http://192.168.0.184:5000/api/CadastrarCaixa';
        const apiConsultaFilial = "http://192.168.0.184:5000/api/relatorioCaixas";
        const apiConsultaMatriz = "http://192.168.0.183:5000/api/relatorioCaixas";
        var empresa = '<?php echo $empresa; ?>';
        var usuario = '<?php echo $usuario; ?>';
        const Token = 'a40016aabcx9';
        const CodCaixa = document.getElementById("InputCodCaixa");
        const DescricaoCaixa = document.getElementById("InputDescricao");
        const TamanhoCaixa = document.getElementById("InputTamanhoCaixa");

        function getFormattedDate(date) {
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            return `${year}-${month}-${day}`;
        }


        function mostrarModalLoading() {
            document.getElementById("modalLoading").style.display = "block";
        }


        function ocultarModalLoading() {
            document.getElementById("modalLoading").style.display = "none";
        }

        function AbrirModal(Modal) {
            document.getElementById(Modal).style.display = 'block';
        };

        function FecharModal(Modal) {
            document.getElementById(Modal).style.display = 'none';
        }

        async function obterCaixas(API, dataInicio, dataFim) {
            mostrarModalLoading();
            try {
                const response = await fetch(`${API}?dataInicio=${dataInicio}&dataFim=${dataFim}`, {
                    method: 'GET',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': Token
                    },
                });

                if (response.ok) {
                    const data = await response.json();
                    console.log(data);
                    CriarTabelaCaixas(data);
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

        function CriarTabelaCaixas(ListaCaixas) {
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
                    { data: 'codcaixa'},
                    { data: 'tamcaixa'},
                    { data: 'quantidade'},
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

            tabela.clear().rows.add(ListaCaixas).draw();

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

        document.getElementById("btnFiltrar").addEventListener('click', ()=> {
            if (empresa === "1") {
                obterCaixas( apiConsultaMatriz,document.getElementById("DataInicial").value, document.getElementById("DataFinal").value);
            } else if (empresa === "4") {
                obterCaixas(apiConsultaFilial, document.getElementById("DataInicial").value, document.getElementById("DataFinal").value);
            }
        })

        window.addEventListener('load', async () => {
            const currentDate = new Date();
            const formattedDate = getFormattedDate(currentDate);
            
            document.getElementById("DataInicial").value = formattedDate;
            document.getElementById("DataFinal").value = formattedDate;

            if (empresa === "1") {

                await obterCaixas(apiConsultaMatriz, document.getElementById("DataInicial").value, document.getElementById("DataFinal").value);

            } else if (empresa === "4") {
                await obterCaixas(apiConsultaFilial,document.getElementById("DataInicial").value, document.getElementById("DataFinal").value);
            }
        });  

        document.getElementById('btnCadastrar').addEventListener('click', () => {
            AbrirModal('ModalCadEmbalagem')
        });

        async function InserirCaixa(api) {
        const Salvar = {
            "codcaixa": CodCaixa.value,
            "nomecaixa": DescricaoCaixa.value,
            "tamanhocaixa": TamanhoCaixa.value
        };

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
            Swal.fire({
                title: "Embalagem Cadastrada",
                icon: "success",
                showConfirmButton: false,
                timer: "3000",
            });
            FecharModal('ModalCadEmbalagem');
            console.log('Teste');

        } else {
            throw new Error('Erro No Retorno');
            Swal.fire({
                title: "Erro",
                icon: "error",
                showConfirmButton: false,
                timer: "3000",
            });
            FecharModal('ModalCadEmbalagem');
        }
    } catch (error) {
        console.error(error);
        Swal.fire({
                title: "Erro",
                icon: "error",
                showConfirmButton: false,
                timer: "3000",
            });
            FecharModal('ModalCadEmbalagem');
    }
};

    document.getElementById('FormCadEmbalagem').addEventListener('submit', function (event) {
        // Prevenir o envio padrão do formulário
        event.preventDefault();

        if (empresa === '1') {
            InserirCaixa(apiCadastroMatriz);
        } else if (empresa === '4') {
            InserirCaixa(apiCadastroFilial);
        }
    });

    </script>
</body>

</html>