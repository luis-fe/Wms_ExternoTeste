const Api = 'http://192.168.0.183:5000/api/GerarCaixa';
const Token = 'a40016aabcx9';
const InputQuantidade = document.getElementById('InputQuantidade')


document.getElementById('BotaoImprimir').addEventListener('click', () =>{
    if(InputQuantidade.value === '') {
        alert("O campo de Quantidade não pode ser vazio!")
    } else {
        CadastrarCaixas(Api);
    }
})


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
            alert('Ok')
            
        } else {
            throw new Error('Erro ao obter os dados da API');
        
        }
    } catch (error) {
        console.error(error);
        alert('Procure o Administrador')
    
    }
}