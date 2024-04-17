<?php
include_once("./templates/cabecalho.php");
?>

<!DOCTYPE html>
<html lang="pt-BR">

<head>
    <link rel="stylesheet" href="./css/CadastroEnderecos.css">
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
            <h3>CADASTRO DE ENDEREÇO</h3>
            <i class="bi bi-x-square-fill" title="Fechar" id="FecharRotina" style="color: black"></i>
        </div>

        <div class="Corpo">
            <div class="CorpoEnderecos">
                <div class="Opcao">
                    <label>
                        <input type="radio" id="radioIncluir" name="acaoEndereco" value="Incluir"> Incluir
                    </label><br>
                    <label>
                        <input type="radio" id="radioExcluir" name="acaoEndereco" value="Excluir"> Excluir
                    </label>
                </div>

                <div class="Comboboxex">
                    <label for="OpcoesNaturezas">Natureza de Estoque:</label>
                    <select id="OpcoesNaturezas">
                        <option></option>
                        <option value="5">5 - P.A ATACADO</option>
                        <option value="7">7 - SALDO</option>
                        <option value="54">54 - BONIFICAÇÃO MKT</option>
                    </select>
                    <label for="OpcoesEstoque">Tipo de Estoque:</label>
                    <select id="OpcoesEstoque">
                        <option></option>
                        <option value="COLECAO">COLEÇÃO</option>
                        <option value="SALDO">SALDO</option>
                    </select>
                </div>
            </div>

            <div class="BotaoCad">
                <button id="ButtonCad">Selecionar Endereços</button>
                <button id="ButtonOps">Endereços Reservados</button>
            </div>

            <div class="LinhaDiv"></div>

            <div class="SelecaoEnderecos">
                <div class="SelecaoInicial">
                    <label for="text" id="LabelRuaInicial">Rua Inicial</label>
                    <input type="text" id="inputRuaInicial">
                    <label for="text" id="LabelModuloInicial">Modulo Inicial</label>
                    <input type="text" id="InputModuloInicial">
                    <label for="text" id="LabelPosicaoInicial">Posicao Inicial</label>
                    <input type="text" id="InputPosicaoInicial">
                </div>

                <div class="SelecaoFinal">
                    <label for="text" id="LabelRuaFinal">Rua Final</label>
                    <input type="text" id="inputRuaFinal">
                    <label for="text" id="LabelModuloFinal">Modulo Final</label>
                    <input type="text" id="InputModuloFinal">
                    <label for="text" id="LabelPosicaoFinal">Posicao Final</label>
                    <input type="text" id="InputPosicaoFinal">
                </div>
            </div>

            <div class="DivBotoes">
                <button id="BotaoPersistir"></button>
                <button id="BotaoCancelar">CANCELAR</button>
            </div>
        </div>
    </div>

    <div class="modalLoading" id="modalLoading">
        <div class="modalLoading-content">
            <div class="loader"></div>
            <p>Aguarde, carregando...</p>
        </div>
    </div>

    <div class="Modal" id="ModalReserva">
        <div class="Modal-Content">
            <div class="Titulo">
                <span class="Close-Modal" onclick="FecharModal('ModalReserva')">&times;</span>
                <h2>Reserva de Endereços</h2>
            </div>
            <div class="FormReserva">
            <form action="" id="FormReserva">
            <div class="SelecaoEnderecosReserva">
                <div class="SelecaoInicial">
                    <label for="text" id="LabelRuaInicial">Rua Inicial</label>
                    <input type="text" id="inputRuaInicialReserva" autocomplete="off" required>
                    <label for="text" id="LabelModuloInicial">Modulo Inicial</label>
                    <input type="text" id="InputModuloInicialReserva" autocomplete="off" required>
                    <label for="text" id="LabelPosicaoInicial">Posicao Inicial</label>
                    <input type="text" id="InputPosicaoInicialReserva" autocomplete="off" required>
                </div>

                <div class="SelecaoFinal">
                    <label for="text" id="LabelRuaFinal">Rua Final</label>
                    <input type="text" id="inputRuaFinalReserva" autocomplete="off" required>
                    <label for="text" id="LabelModuloFinal">Modulo Final</label>
                    <input type="text" id="InputModuloFinalReserva" autocomplete="off" required>
                    <label for="text" id="LabelPosicaoFinal">Posicao Final</label>
                    <input type="text" id="InputPosicaoFinalReserva" autocomplete="off" required>
                </div>
            </div>
                <div class="buttonedit">
                    <button type="submit" id="SalvarEditUser">Salvar</button>
                </div>
            </form>
            </div>
           
        </div>
    </div>

    <script>
        const CadastrarEnderecosMatriz = "http://192.168.0.183:5000/api/EnderecoAtacado";
        const CadastrarEnderecosFilial = "http://177.221.240.74:5000/api/EnderecoAtacado";
        var empresa = '<?php echo $empresa; ?>';
        var usuario = '<?php echo $usuario; ?>';
        const Token = 'a40016aabcx9';


        function AbrirModal(Modal) {
            document.getElementById(Modal).style.display = 'block';
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



        function CadastrarEnderecos(api, Metodo, Condicao, texto, Reserva, RuaInicial, RuaFinal, ModuloInicial, ModuloFinal, PosicaoInicial, PosicaoFinal) {
            mostrarModalLoading();

            

            const ruaInicial = document.getElementById(RuaInicial);
            const ruaFinal = document.getElementById(RuaFinal);
            const moduloInicial = document.getElementById(ModuloInicial);
            const moduloFinal = document.getElementById(ModuloFinal);
            const posicaoInicial = document.getElementById(PosicaoInicial);
            const posicaoFinal = document.getElementById(PosicaoFinal);
            const Natureza = document.getElementById("OpcoesNaturezas");
            const TipoEstoque = document.getElementById("OpcoesEstoque");

            const Dados = {
                "ruaInicial": ruaInicial.value,
                "ruaFinal": ruaFinal.value,
                "modulo": moduloInicial.value,
                "moduloFinal": moduloFinal.value,
                "posicao": posicaoInicial.value,
                "posicaoFinal": posicaoFinal.value,
                "tipo": TipoEstoque.value,
                "natureza": Natureza.value,
                "empresa": empresa,
                "imprimir": Condicao,
                "enderecoReservado": Reserva
            }


            fetch(api, {

                method: Metodo,
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'a40016aabcx9'
                },
                body: JSON.stringify(Dados),
            })

                .then(response => {
                    if (response.ok) {
                        return response.json();
                    } else {
                        throw new Error('Erro ao obter a lista de usuários');
                        Swal.fire({
                        title: "Erro",
                        icon: "error",
                        showConfirmButton: false,
                        timer: "3000",
                    });
                    }
                })
                .then(data => {
                    console.log(data);
                    ocultarModalLoading();
                    Swal.fire({
                        title: `${texto}`,
                        icon: "success",
                        showConfirmButton: false,
                        timer: "3000",
                    });
                })
                .catch(error => {
                    console.error(error);
                    ocultarModalLoading();
                    Swal.fire({
                        title: "Erro",
                        icon: "error",
                        showConfirmButton: false,
                        timer: "3000",
            });
                });
        }



        const BotaoSelecionar = document.getElementById("ButtonCad");
        const Natureza = document.getElementById("OpcoesNaturezas");
        const TipoEstoque = document.getElementById("OpcoesEstoque");
        const radioIncluir = document.getElementById("radioIncluir");
        const radioExcluir = document.getElementById("radioExcluir");
        const BotaoPersistir = document.getElementById("BotaoPersistir");
        const BotaoCancelar = document.getElementById("BotaoCancelar");

        BotaoSelecionar.addEventListener('click', () => {
            const SelecaoEnderecos = document.getElementsByClassName("SelecaoEnderecos")[0];
            const DivBotoes = document.getElementsByClassName("DivBotoes")[0];

            if (radioIncluir.checked) {
                if (TipoEstoque.value !== "" && Natureza.value !== "") {
                    Natureza.disabled = true;
                    TipoEstoque.disabled = true;
                    radioIncluir.disabled = true;
                    radioExcluir.disabled = true;
                    DivBotoes.style.display = "flex";
                    BotaoPersistir.textContent = "CADASTRAR"
                    SelecaoEnderecos.style.display = "flex";
                    TipoEstoque.style.borderColor = "";
                    Natureza.style.borderColor = "";
                } else {
                    if (TipoEstoque.value === "") {
                        TipoEstoque.style.borderColor = "red";
                    } else {
                        TipoEstoque.style.borderColor = "";
                    }

                    if (Natureza.value === "") {
                        Natureza.style.borderColor = "red";
                    } else {
                        Natureza.style.borderColor = "";
                    }

                    setTimeout(() => {
                        TipoEstoque.style.borderColor = "";
                        Natureza.style.borderColor = "";
                    }, 10000);
                }
            } else if (radioExcluir.checked) {
                if (TipoEstoque.value !== "" && Natureza.value !== "") {
                    Natureza.disabled = true;
                    TipoEstoque.disabled = true;
                    radioIncluir.disabled = true;
                    radioExcluir.disabled = true;
                    DivBotoes.style.display = "flex";
                    BotaoPersistir.textContent = "EXCLUIR"
                    SelecaoEnderecos.style.display = "flex";
                    TipoEstoque.style.borderColor = "";
                    Natureza.style.borderColor = "";
                } else {
                    if (TipoEstoque.value === "") {
                        TipoEstoque.style.borderColor = "red";
                    } else {
                        TipoEstoque.style.borderColor = "";
                    }

                    if (Natureza.value === "") {
                        Natureza.style.borderColor = "red";
                    } else {
                        Natureza.style.borderColor = "";
                    }

                    setTimeout(() => {
                        TipoEstoque.style.borderColor = "";
                        Natureza.style.borderColor = "";
                    }, 10000);
                }
            } else {
                alert("Favor selecione a opção Incluir ou Excluir!");
            }
        });


        const BotaoPersistir1 = document.getElementById("BotaoPersistir");
        const SelecaoEnderecos = document.getElementsByClassName("SelecaoEnderecos")[0];
        const DivBotoes = document.getElementsByClassName("DivBotoes")[0];
        BotaoPersistir1.addEventListener('click', () => {
            const inputsSelecao = document.querySelectorAll(".SelecaoEnderecos input[type='text']");

            let inputsVazias = false;

            inputsSelecao.forEach(input => {
                if (input.value.trim() === "") {
                    input.style.borderColor = "red";
                    inputsVazias = true;

                    // Define um timeout para remover a borda vermelha após 10 segundos
                    setTimeout(() => {
                        input.style.borderColor = "";
                    }, 10000); // 10000 milissegundos = 10 segundos
                } else {
                    input.style.borderColor = ""; // Limpa a borda se o campo não estiver vazio
                }
            });

            if (inputsVazias) {
                return; // Retorna se houver campos vazios
            }


            if (radioIncluir.checked) {
                Swal.fire({
                    title: "Deseja Imprimir os Endereços?",
                    text: "",
                    icon: "warning",
                    showCancelButton: true,
                    confirmButtonColor: "#3085d6",
                    cancelButtonColor: "#d33",
                    confirmButtonText: "Sim",
                    cancelButtonText: "Não"
                }).then((result) => {
                    if (result.isConfirmed) {
                        Imprimir();
                } else {
                    NaoImprimir();
                }
            });

            } else if (radioExcluir.checked) {
                if (empresa === "1") {
                    CadastrarEnderecos(CadastrarEnderecosMatriz, "DELETE", false, "Endereços Excluidos", "", "inputRuaInicial", "inputRuaFinal", "InputModuloInicial", "InputModuloFinal", "InputPosicaoInicial", "InputPosicaoFinal");
                } else if (empresa === "4") {
                    CadastrarEnderecos(CadastrarEnderecosFilial, "DELETE", false, "Endereços Excluidos", "", "inputRuaInicial", "inputRuaFinal", "InputModuloInicial", "InputModuloFinal", "InputPosicaoInicial", "InputPosicaoFinal");
                }


                const inputsSelecao = document.querySelectorAll(".SelecaoEnderecos input[type='text']");
                inputsSelecao.forEach(input => {
                    input.value = "";
                    Natureza.value = ""; // Limpa a seleção da combobox Natureza
                    TipoEstoque.value = ""; // Limpa a seleção da combobox TipoEstoque
                    radioIncluir.checked = false; // Desmarca a rádio Incluir
                    radioExcluir.checked = false; // Desmarca a rádio Excluir

                    Natureza.disabled = false;
                    TipoEstoque.disabled = false;
                    radioIncluir.disabled = false;
                    radioExcluir.disabled = false;
                    DivBotoes.style.display = "none";
                    BotaoPersistir.textContent = "";
                    SelecaoEnderecos.style.display = "none";
                    TipoEstoque.style.borderColor = "";
                    Natureza.style.borderColor = "";
                })
            }

        });

       function Imprimir(){
            if (empresa === "1") {
                CadastrarEnderecos(CadastrarEnderecosMatriz, "PUT", true, "Endereços Cadastrados", "", "inputRuaInicial", "inputRuaFinal", "InputModuloInicial", "InputModuloFinal", "InputPosicaoInicial", "InputPosicaoFinal");
            } else if (empresa === "4") {
                CadastrarEnderecos(CadastrarEnderecosFilial, "PUT", true, "Endereços Cadastrados", "", "inputRuaInicial", "inputRuaFinal", "InputModuloInicial", "InputModuloFinal", "InputPosicaoInicial", "InputPosicaoFinal");
            }
            const inputsSelecao = document.querySelectorAll(".SelecaoEnderecos input[type='text']");
            inputsSelecao.forEach(input => {
                input.value = "";
                Natureza.value = ""; // Limpa a seleção da combobox Natureza
                TipoEstoque.value = ""; // Limpa a seleção da combobox TipoEstoque
                radioIncluir.checked = false; // Desmarca a rádio Incluir
                radioExcluir.checked = false; // Desmarca a rádio Excluir

                Natureza.disabled = false;
                TipoEstoque.disabled = false;
                radioIncluir.disabled = false;
                radioExcluir.disabled = false;
                DivBotoes.style.display = "none";
                BotaoPersistir.textContent = "";
                SelecaoEnderecos.style.display = "none";
                TipoEstoque.style.borderColor = "";
                Natureza.style.borderColor = "";
            })
        };



        function NaoImprimir() {
            if (empresa === "1") {
                CadastrarEnderecos(CadastrarEnderecosMatriz, "PUT", false, "Endereços Cadastrados", "", "inputRuaInicial", "inputRuaFinal", "InputModuloInicial", "InputModuloFinal", "InputPosicaoInicial", "InputPosicaoFinal");
            } else if (empresa === "4") {
                CadastrarEnderecos(CadastrarEnderecosFilial, "PUT", false, "Endereços Cadastrados", "", "inputRuaInicial", "inputRuaFinal", "InputModuloInicial", "InputModuloFinal", "InputPosicaoInicial", "InputPosicaoFinal");
            }
            const inputsSelecao = document.querySelectorAll(".SelecaoEnderecos input[type='text']");
            inputsSelecao.forEach(input => {
                input.value = "";
                Natureza.value = ""; // Limpa a seleção da combobox Natureza
                TipoEstoque.value = ""; // Limpa a seleção da combobox TipoEstoque
                radioIncluir.checked = false; // Desmarca a rádio Incluir
                radioExcluir.checked = false; // Desmarca a rádio Excluir

                Natureza.disabled = false;
                TipoEstoque.disabled = false;
                radioIncluir.disabled = false;
                radioExcluir.disabled = false;
                DivBotoes.style.display = "none";
                BotaoPersistir.textContent = "";
                SelecaoEnderecos.style.display = "none";
                TipoEstoque.style.borderColor = "";
                Natureza.style.borderColor = "";
            });
        }

        document.getElementById('BotaoCancelar').addEventListener('click', () => {
            const inputsSelecao = document.querySelectorAll(".SelecaoEnderecos input[type='text']");
            inputsSelecao.forEach(input => {
                input.value = "";
                Natureza.value = ""; // Limpa a seleção da combobox Natureza
                TipoEstoque.value = ""; // Limpa a seleção da combobox TipoEstoque
                radioIncluir.checked = false; // Desmarca a rádio Incluir
                radioExcluir.checked = false; // Desmarca a rádio Excluir

                Natureza.disabled = false;
                TipoEstoque.disabled = false;
                radioIncluir.disabled = false;
                radioExcluir.disabled = false;
                DivBotoes.style.display = "none";
                BotaoPersistir.textContent = "";
                SelecaoEnderecos.style.display = "none";
                TipoEstoque.style.borderColor = "";
                Natureza.style.borderColor = "";
            });
        });

        document.getElementById('ButtonOps').addEventListener('click', () => {

            AbrirModal('ModalReserva');
        })

        document.getElementById('FormReserva').addEventListener('submit', function (event) {
        // Prevenir o envio padrão do formulário
        event.preventDefault();

        if (empresa === '1') {
            CadastrarEnderecos(CadastrarEnderecosMatriz, "PUT", false, "Endereços Cadastrados", "sim", "inputRuaInicialReserva", "inputRuaFinalReserva", "InputModuloInicialReserva", "InputModuloFinalReserva", "InputPosicaoInicialReserva", "InputPosicaoFinalReserva");
        } else if (empresa === '4') {
            CadastrarEnderecos(CadastrarEnderecosMatriz, "PUT", false, "Endereços Cadastrados", "sim", "inputRuaInicialReserva", "inputRuaFinalReserva", "InputModuloInicialReserva", "InputModuloFinalReserva", "InputPosicaoInicialReserva", "InputPosicaoFinalReserva");
        }
        });

    </script>
</body>

</html>