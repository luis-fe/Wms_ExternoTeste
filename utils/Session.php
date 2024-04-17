<?php
// TelaInicial.php

session_start();

// Verifica se o usuário não está logado
if (!isset($_SESSION['usuario'])) {
    // Redireciona para a página de login
    header("Location: indexWms.php");
    exit(); // Certifica-se de que o script é encerrado após o redirecionamento
}

?>