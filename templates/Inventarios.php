<?php
include_once("./templates/cabecalho.php");
?>

<!DOCTYPE html>
<html lang="pt-BR">

<head>
    <link rel="stylesheet" href="./css/Wms.css">
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

    .Datas{
        width: 100%
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

      select{
        width: 100px;
        padding: 5px
      }

      .tabela {
    width: 99%;
    min-height: 65%;
    max-height: 65%;
    padding: auto;
    margin: auto;
    overflow: auto;
}

.Informacoes{
    margin-top: 10px;
    width: 100%
}

.InfoItem{
    width: 50%;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center
}

.InfoItem label{
    padding-right: 5px;
    padding-left: 5px;
    text-align: center
}

</style>

<body class="<?php echo $classe_empresa; ?>">
    <div class="Container">
        <div class="title">
            <h3>RELATÓRIO DE INVENTÁRIO</h3>
            <i class="bi bi-x-square-fill" title="Fechar" id="FecharRotina" style="color: black"></i>
        </div>

        <div class="Corpo">
        <div class="Datas">
                <label for="text">Natureza</label>
                <select style="margin-right: 20px;" id="SelectNaturezas">
                    <option value="5">5</option>
                    <option value="7">7</option>
                    <option value="">Ambas</option>
                </select>
                <label for="text">Data Inicio</label>
                <input type="date" id="DataInicial">
                <label for="text">Data Fim</label>
                <input type="date" id="DataFinal">
                <button id="btnFiltrar">Consultar</button>
            </div>
            <div class="tabela">
                <table id="Tabela" class="Tabela">
                    <thead>
                        <tr>
                            <th>Rua</th>
                            <th>Quantidade Endereços</th>
                            <th>Status</th>
                            <th>% Realizado</th>
                        </tr>
                    </thead>
                    <tbody id="tBodyTabela">
                    </tbody>
                </table>
            </div>
            <div class="Informacoes" id="Infomacoes1">
        <div class="InfoItem">
            <label id="LabelPratTotal" for="">Endereços Totais:</label>
            <label id="LabelResultPratTotal" for="text" style="font-size: 30px; background-color: rgb(17, 45, 126); color: white; border-radius:5px;"></label>

            <label id="LabelPratInventariadas" for="text">Endereços Inventariados:</label>
            <label id="LabelResultPratInventariadas" for="text" style="font-size: 30px; background-color: rgb(17, 45, 126); border-radius:5px; color:white;"></label>
        </div>
    
     
        <div class="InfoItem">
            <label id="LabelPecasTotais" for="">Peças Totais:</label>
            <label id="LabelResultPecasTotais" for="text" style="font-size: 30px; background-color: rgb(17, 45, 126); color: white; border-radius:5px;"></label>

            <label id="LabelPecasInventariadas" for="text">Peças Inventariadas:</label>
            <label id="LabelResultPecasInventariadas" for="text" style="font-size: 30px; background-color: rgb(17, 45, 126); color: white; border-radius:5px;"></label>
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
        const ApiMatriz = 'http://192.168.0.183:5000/api/RelatorioInventario?';
        const ApiFilial = 'http://192.168.0.184:5000/api/RelatorioInventario?';var empresa = '<?php echo $empresa; ?>';
        var usuario = '<?php echo $usuario; ?>';
        const Token = 'a40016aabcx9';


        function getFormattedDate(date) {
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
        }

        function formatarDados(data) {
            return data.map(item => {
                return {
                    'Rua': item['Rua'],
                    'Qtd Prateleiras': item['Qtd Prat.'],
                    'status': item['status'],
                    '% Realizado': item['% Realizado'],
                };
            });
        }

        function mostrarModalLoading() {
            document.getElementById("modalLoading").style.display = "block";
        }


        function ocultarModalLoading() {
            document.getElementById("modalLoading").style.display = "none";
        }

        async function obterRelatorio(API, natureza, dataInicio, DataFim) {
            mostrarModalLoading();
            try {
                const response = await fetch(`${API}natureza=${natureza}&datainicio=${dataInicio}&datafinal=${DataFim}`, {
                    method: 'GET',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': Token
                    },
                });

                if (response.ok) {
                    const data = await response.json();
                    console.log(data);
                    const dadosFormatados = formatarDados(data[0]['5- Detalhamento Ruas:']);
                    CriarTabelaRelatorio(dadosFormatados);
                    document.getElementById("LabelResultPratTotal").textContent = data[0]['3 - Total Enderecos'];
                    document.getElementById("LabelResultPratInventariadas").textContent = data[0]['4- Enderecos Inventariados'];
                    document.getElementById("LabelResultPecasTotais").textContent = data[0]['1: Total de Peças'];
                    document.getElementById("LabelResultPecasInventariadas").textContent = data[0]['2- Pçs Inventariadas'];
                    document.getElementById('Infomacoes1').style.display = 'flex';
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

            if ($.fn.DataTable.isDataTable('#Tabela')) {
                $('#Tabela').DataTable().destroy();
            }

            tabela = $('#Tabela').DataTable({
                paging: false,
                info: false,
                searching: true,
                colReorder: true,
                colResize: true,
                columns: [
                    { data: 'Rua' },
                    { data: 'Qtd Prateleiras' },
                    { data: 'status' },
                    { data: '% Realizado' },
                ],
            });

            tabela.clear().rows.add(ListaRelatorio).draw();

        };



        window.addEventListener('load', async () => {
            const currentDate = new Date();
            const formattedDate = getFormattedDate(currentDate);
            
            document.getElementById("DataInicial").value = formattedDate;
            document.getElementById("DataFinal").value = formattedDate;

            if (empresa === "1") {

                await obterRelatorio(ApiMatriz, document.getElementById("SelectNaturezas").value, document.getElementById("DataInicial").value, document.getElementById("DataFinal").value);

            } else if (empresa === "4") {
                await obterRelatorio(ApiFilial, document.getElementById("SelectNaturezas").value, document.getElementById("DataInicial").value, document.getElementById("DataFinal").value);
            }
        });  

        document.getElementById('btnFiltrar').addEventListener('click', async () => {
            if (empresa === "1") {

                await obterRelatorio(apiConsultaMatriz, document.getElementById("SelectNaturezas").value, document.getElementById("DataInicial").value, document.getElementById("DataFinal").value);

            } else if (empresa === "4") {
                await obterRelatorio(apiConsultaFilial, document.getElementById("SelectNaturezas").value, document.getElementById("DataInicial").value, document.getElementById("DataFinal").value);
            }
        });  
    </script>
</body>

</html>