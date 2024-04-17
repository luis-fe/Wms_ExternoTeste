<?php
    include_once("./helpers/url.php");
    include_once("./utils/FunctionsLogin.php");

    if ($_SERVER["REQUEST_METHOD"] == "POST") {

        $username = $_POST["username"];
        $password = $_POST["password"];
        $empresa = $_POST["empresa"];
        
    
        $Resposta = fazerChamadaApi($username, $password, $empresa);

        if ($Resposta['status'] == true) {
            $nome = $Resposta['nome'];
            session_start();
            $_SESSION['usuario'] = $nome;
            $_SESSION['empresa'] = $empresa;
            header("Location: CadastroUsuario.php");
            exit();
        } else {
            $mensagemErro = "Usuário inválido. Tente novamente.";
        }
    }
?>

<!DOCTYPE html>
<html lang="pt-BR">

<head>
    <link rel="stylesheet" href="./css/LoginWms.css">
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Portal Wms</title>

</head>

<body>
    <section>
        <div class="form-box">
            <img src="./Imagens/Icone Wms.png" alt="">
            <div class="form-value">
                <form action="" method="POST">
                    <div class="inputbox">
                        <ion-icon name="mail-outline"></ion-icon>
                        <input type="text" name="username" required>
                        <label for="">Usuário</label>
                    </div>
                    <div class="inputbox">
                        <ion-icon name="lock-closed-outline"></ion-icon>
                        <input type="password" name="password" required>
                        <label for="">Senha</label>
                    </div>
                    <div class="inputbox">
                        <select id="InputEmpresa" name="empresa" class="form-control" required>
                            <option value="" disabled selected>Selecione a Empresa</option>
                            <option value="1">Matriz-GO</option>
                            <option value="4">Filial-PR</option>
                        </select>
                    </div>
                    <button>Acessar</button>
                    <?php if (isset($mensagemErro)) : ?>
                    <p style="color: red;">
                        <?php echo $mensagemErro; ?>
                    </p>
                    <?php endif; ?>
                </form>
            </div>
        </div>
    </section>
    <script type="module" src="https://unpkg.com/ionicons@7.1.0/dist/ionicons/ionicons.esm.js"></script>
    <script nomodule src="https://unpkg.com/ionicons@7.1.0/dist/ionicons/ionicons.js"></script>
</body>

</html>