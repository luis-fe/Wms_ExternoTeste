<?php
include_once("./templates/cabecalho.php");
?>

<!DOCTYPE html>
<html lang="pt-BR">

<head>
    <link rel="stylesheet" href="./css/ContainerWms.css">
    <link rel="stylesheet" href="./css/Modais.css">
    <link rel="stylesheet" href="./css/QrCodes.css">
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
        0% {
            transform: rotate(0deg);
        }

        100% {
            transform: rotate(360deg);
        }
    }
</style>

<body class="<?php echo $classe_empresa; ?>">
    <div class="Container">
        <div class="title">
            <h3>IMPRESSÃO QR CODES</h3>
            <i class="bi bi-x-square-fill" title="Fechar" id="FecharRotina" style="color: black"></i>
        </div>

        <div class="Corpo">
            <div class="SelecaoEnderecos">
                <label for="text" id="LabelQuantidade">Quantidade de Caixas</label>
                <input type="text" id="InputQuantidade">
                <button id="BotaoImprimir">Imprimir</button>
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
        const Api = 'http://192.168.0.183:5000/api/GerarCaixa';
        var empresa = '<?php echo $empresa; ?>';
        var usuario = '<?php echo $usuario; ?>';
        const Token = 'a40016aabcx9';
        const InputQuantidade = document.getElementById('InputQuantidade');



        function mostrarModalLoading() {
            document.getElementById("modalLoading").style.display = "block";
        }


        function ocultarModalLoading() {
            document.getElementById("modalLoading").style.display = "none";
        }




        document.getElementById('BotaoImprimir').addEventListener('click', () => {
            if (InputQuantidade.value === '') {
                Swal.fire({
                    title: "O campo está Vazio!",
                    icon: "error",
                    showConfirmButton: false,
                    timer: "3000",
                });
            } else {
                CadastrarCaixas(Api);
            }
        });


        async function CadastrarCaixas(Api) {
            dados = {
                "QuantidadeImprimir": InputQuantidade.value
            }

            try {
                const response = await fetch(Api, {
                    method: 'PUT',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': Token
                    },
                    body: JSON.stringify(dados),
                });

                if (response.ok) {
                    const data = await response.json();
                    Swal.fire({
                    title: "QrCodes Imprimidos",
                    icon: "success",
                    showConfirmButton: false,
                    timer: "3000",
                });

                } else {
                    throw new Error('Erro ao obter os dados da API');
                    Swal.fire({
                    title: "Erro",
                    icon: "error",
                    showConfirmButton: false,
                    timer: "3000",
                });

                }
            } catch (error) {
                console.error(error);
                Swal.fire({
                    title: "Erro",
                    icon: "error",
                    showConfirmButton: false,
                    timer: "3000",
                });

            }
        }
    </script>
</body>

</html>