const ApiAtribuicao = 'http://192.168.0.183:5000/api/AtribuirPedidos'
const Atribuicao = {
    "codUsuario": "",
    "pedidos": [306690],
    "data": "2023-06-22 08:00:00"
}
function AtribuicaoPedidos() {
    fetch(ApiAtribuicao, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'a40016aabcx9'
        },
        body: JSON.stringify(Atribuicao),
    })
    .then(response => {
        if (response.ok) {
            return response.json();
        } else {
            throw new Error('Erro ao obter a lista de usuários');
        }
    })
    .then(data => {
        console.log(data)
    })
    .catch(error => {
        console.error(error);
        FecharModalLoading();
    });
}

AtribuicaoPedidos()