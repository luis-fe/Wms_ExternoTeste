
<?php

include_once("./utils/Session.php");


// Defina uma classe padrão
$classe_empresa = '';

// Verifique se a empresa está definida na sessão
if (isset($_SESSION['empresa'])) {
    $usuario = $_SESSION['usuario'];
    $empresa = $_SESSION['empresa'];
    $classe_empresa = ($empresa == 1) ? 'Matriz' : 'Filial';
}
?>

<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="./css/Cabecalho.css">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.5/font/bootstrap-icons.css">
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.10.25/css/jquery.dataTables.css">
    <script type="text/javascript" charset="utf8" src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script type="text/javascript" charset="utf8" src="https://cdn.datatables.net/1.10.25/js/jquery.dataTables.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/sweetalert2@11.10.7/dist/sweetalert2.all.min.js"></script>
    <link href="https://cdn.jsdelivr.net/npm/sweetalert2@11.10.7/dist/sweetalert2.min.css" rel="stylesheet">
    <title>Portal WMS</title>
</head>

<body class="<?php echo $classe_empresa; ?>">
<div class="header">
        <i class="bi bi-list" id="icon-menu"></i>
        <h3>Portal WMS</h3>
    </div>
<div class="Menu" id="Menu">
    <div class="sidebar">
    <ul>
            <li>
                <a href="#">
                    <i class="bi bi-folder-fill"></i> 
                    CADASTROS
                    <div class="teste">
                        <i class="bi bi-chevron-right"></i>
                        <i class="bi bi-chevron-down" style="display: none"></i>
                    </div>
                </a>
                <ul>
                    <li><a  href="./CadastroQrCode.php">CADASTRO QR CODE DAS CAIXAS</a></li>
                    <li><a  href="./CadastroEnderecos.php">CADASTRO DE ENDEREÇOS</a></li>
                </ul>
            </li>
        </ul>

        <ul>
            <li>
                <a href="#">
                    <i class="bi bi-folder-fill"></i> 
                    CONFIGURAÇÕES
                    <div class="teste">
                        <i class="bi bi-chevron-right"></i>
                        <i class="bi bi-chevron-down" style="display: none"></i>
                    </div>
                </a>
                <ul>
                    <li><a  href="./CadastroUsuarioWms.php">GERENCIAMENTO DE USUÁRIOS</a></li>
                    <li><a  href="./GerenciamentoChamados.php">GERENCIAMENTO DE CHAMADOS</a></li>
                </ul>
            </li>
        </ul>

        <ul>
            <li>
                <a href="#">
                    <i class="bi bi-folder-fill"></i> 
                    PRÉ-FATURAMENTO
                    <div class="teste">
                        <i class="bi bi-chevron-right"></i>
                        <i class="bi bi-chevron-down" style="display: none"></i>
                    </div>
                </a>
                <ul>
                    <li><a  href="./ReservaDePedidos.php">RESERVA DE PEDIDOS</a></li>
                    <li><a  href="./GerenciamentoLinhas.php">MONITOR DE PEDIDOS</a></li>
                </ul>
            </li>
        </ul>

        <ul>
            <li>
                <a href="#">
                    <i class="bi bi-folder-fill"></i> 
                    RELATÓRIOS
                    <div class="teste">
                        <i class="bi bi-chevron-right"></i>
                        <i class="bi bi-chevron-down" style="display: none"></i>
                    </div>
                </a>
                <ul>
                    <li><a  href="./FilaReposicao.php">FILA DE REPOSIÇÃO</a></li>
                    <li><a  href="./DistribuicaoDePedidos.php">DISTRIBUIÇÃO DE PEDIDOS</a></li>
                    <li><a  href="./ConsumoDeEmbalagens.php">CONSUMO DE EMBALAGENS</a></li>
                    <li><a  href="./Inventarios.php">INVENTÁRIO</a></li>
                    <li><a  href="./TagsX_Estoque.php">TAG'S x FÍSICO</a></li>
                    <li><a  href="./Substitutos.php">ANÁLISE DE SUBSTITUTOS</a></li>
                </ul>
            </li>
        </ul>

        <ul>
            <li>
                <a href="LogoutWms.php">
                <i class="bi bi-power" style="color: red; font-size: 20px; font-weight: bold;"></i>
                    SAIR
                </a>
            </li>
        </ul>
    </div>

</div>
</body>
<script>
    const Menu = document.getElementById('Menu');
    const IconMenu = document.getElementById('icon-menu');
    let MenuVisible = false;

    IconMenu.addEventListener('click', () => {
        MenuVisible = !MenuVisible;

        if (MenuVisible) {
            Menu.style.display = 'block';
            setTimeout(() => {
                Menu.classList.add('active');
            }, 10);
        } else {
            Menu.classList.remove('active');
            setTimeout(() => {
                Menu.style.display = 'none';
            }, 500); // Ajuste o tempo de acordo com a duração da transição no CSS
        }
    });

    const menuItems = document.querySelectorAll('.sidebar ul li');

menuItems.forEach(item => {
    item.addEventListener('click', () => {
        const submenu = item.querySelector('ul');

        if (submenu) {
            submenu.style.display = submenu.style.display === 'block' ? 'none' : 'block';

            const chevronDown = item.querySelector('.bi-chevron-down');
            const chevronRight = item.querySelector('.bi-chevron-right');

            if (submenu.style.display === 'block') {
                chevronDown.style.display = 'inline';
                chevronRight.style.display = 'none';
            } else {
                chevronDown.style.display = 'none';
                chevronRight.style.display = 'inline';
            }
        }
    });
});


</script>
</html>
