let MetaApi = "";
let RealizadoApi = "";
let PecasFase = "";
let PecasFila = "";
let PecasRepostasApi = "";
let EnderecosTotais = "";
let EnderecosUtilizados = "";
let PedidosRetorna = "";
let PedidosCompletos = "";
const Api = 'http://192.168.0.183:5000/api/RelatorioTotalFila';
const ApiEnderecos = 'http://192.168.0.183:5000/api/DisponibilidadeEnderecos?';
const ApiPedidos = 'http://192.168.0.183:5000/api/statuspedidos';
const Token = 'a40016aabcx9';
const PecasRetorna = document.getElementById("PecasRetorna");
const PecasRepostas = document.getElementById("PecasRepostas");
const PecasFase1 = document.getElementById("PecasReposicaoTotal");
const PecasRepostas1 = document.getElementById("FilaReposta");
const EnderecosTotal = document.getElementById("EnderecosCadastrados");
const EnderecosUtilizado = document.getElementById("EnderecosUtilizados");
const PedidoRetorna = document.getElementById("PedidosRetorna");
const PedidoCompleto = document.getElementById("TotalPedidosCompletos");


async function ChamadaApi(api, callback) {
    try {
        const response = await fetch(api, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': Token
            },
        });

        if (response.ok) {
             const data = await response.json();
            MetaApi = data[0]["2.1- Total de Skus nos Pedidos em aberto "];
            MetaApi = MetaApi.replace(/\./, '');
            MetaApi = MetaApi.replace(/\ pçs/, '');
            RealizadoApi = data[0]["2.3-Qtd de Enderecos OK Reposto nos Pedido"];
            RealizadoApi = RealizadoApi.replace(/\./, '');
            RealizadoApi = RealizadoApi.replace(/\ pçs/, '');
            PecasFase = data[0]["1.1-Total de Peças Nat. 5"];
            PecasFase = PecasFase.replace(/\./, '');
            PecasFase = PecasFase.replace(/\ pçs/, '');
            PecasRepostasApi = data[0]["1.3-Peçs Repostas"];
            PecasRepostasApi = PecasRepostasApi.replace(/\./, '');
            PecasRepostasApi = PecasRepostasApi.replace(/\ pçs/, '');
            PecasFila = data[0]["1.2-Saldo na Fila"];
            PecasFila = PecasFila.replace(/\./, '');
            PecasFila = PecasFila.replace(/\ pçs/, '');
            PecasRetorna.textContent = parseInt(MetaApi).toLocaleString('pt-BR');
            PecasRepostas.textContent = parseInt(RealizadoApi).toLocaleString('pt-BR');
            PecasFase1.textContent = parseInt(PecasFase).toLocaleString('pt-BR');
            PecasRepostas1.textContent = parseInt(PecasRepostasApi).toLocaleString('pt-BR');
            DiferencaReposicao.textContent = (parseInt(PecasFase)-parseInt(PecasRepostasApi)).toLocaleString('pt-BR');
            DiferencaPecasPedidos.textContent = (parseInt(MetaApi)-parseInt(RealizadoApi)).toLocaleString('pt-BR');
            console.log(MetaApi)
            console.log(RealizadoApi)

            // Obtém todos os elementos com a classe "Gerenciamento"
            const elementosGerenciamento = document.getElementsByClassName("Gerenciamento");

            // Define o estilo para cada elemento da coleção
            for (let i = 0; i < elementosGerenciamento.length; i++) {
            elementosGerenciamento[i].style.display = "flex";
}

            callback(); // Chama a função de criação do gráfico após a conclusão da chamada
        } else {
            throw new Error('Erro No Retorno');
        }
    } catch (error) {
        console.error(error);
    }
}

async function ChamadaApiEnderecos(api,Empresa, Natureza) {
    try {
        const response = await fetch(`${api}empresa=${Empresa}&natureza=${Natureza}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': Token
            },
        });

        if (response.ok) {
            const data = await response.json();
            EnderecosTotais = data[0]["1- Total de Enderecos Natureza "];
            EnderecosTotais = EnderecosTotais.replace(/\./, '');
            EnderecosUtilizados = data[0]["2- Total de Enderecos Disponiveis"];
            EnderecosUtilizados = EnderecosUtilizados.replace(/\./, '');
            EnderecosUtilizados = EnderecosTotais - EnderecosUtilizados;
            EnderecosTotal.textContent = parseInt(EnderecosTotais).toLocaleString('pt-BR');
            EnderecosUtilizado.textContent = parseInt(EnderecosUtilizados).toLocaleString('pt-BR');
            console.log(MetaApi)
            console.log(RealizadoApi)
        } else {
            throw new Error('Erro No Retorno');
        }
    } catch (error) {
        console.error(error);
    }
}

async function ChamadaApiPedidos(api) {
    try {
        const response = await fetch(`${api}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': Token
            },
        });

        if (response.ok) {
            const data = await response.json();
            PedidosRetorna = data[0]["1. Total de Pedidos no Retorna"];
            PedidosRetorna = PedidosRetorna.replace(/\./, '');
            PedidosCompletos = data[0]["2. Total de Pedidos fecham 100%"];
            PedidosCompletos = PedidosCompletos.replace(/\./, '');
            PedidoCompleto.textContent = parseInt(PedidosCompletos).toLocaleString('pt-BR');
            PedidoRetorna.textContent = parseInt(PedidosRetorna).toLocaleString('pt-BR');
        } else {
            throw new Error('Erro No Retorno');
        }
    } catch (error) {
        console.error(error);
    }
}




function CriarGrafico() {
    const container = document.getElementById('medidorSeparacao');
    const containerReposicao = document.getElementById('medidorReposicao');
    const containerEnderecos = document.getElementById('medidorEnderecos');
    const containerPedidos = document.getElementById('medidorPedidos');
    const bar = new ProgressBar.Circle(container, {
        strokeWidth: 10, // Largura da barra
        easing: 'easeInOut',
        duration: 1400,
        color: 'rgb(17, 45, 126)', // Cor da barra
        trailColor: 'rgb(93, 140, 233)', // Cor do fundo da barra
        trailWidth: 10, // Largura do fundo da barra
        svgStyle: null,
        text: {

            value: `${(RealizadoApi / MetaApi * 100).toFixed(2)}%`,
         
            
        
    }
    });

    const barReposicao = new ProgressBar.Circle(containerReposicao, {
        strokeWidth: 10, // Largura da barra
        easing: 'easeInOut',
        duration: 1400,
        color: 'rgb(17, 45, 126)', // Cor da barra
        trailColor: 'rgb(93, 140, 233)', // Cor do fundo da barra
        trailWidth: 10, // Largura do fundo da barra
        svgStyle: null,
        text: {

            value: `${(PecasRepostasApi / PecasFase * 100).toFixed(2)}%`,
          
        }
    });

    const barEnderecos = new ProgressBar.Circle(containerEnderecos, {
        strokeWidth: 10, // Largura da barra
        easing: 'easeInOut',
        duration: 1400,
        color: 'rgb(17, 45, 126)', // Cor da barra
        trailColor: 'rgb(93, 140, 233)', // Cor do fundo da barra
        trailWidth: 10, // Largura do fundo da barra
        svgStyle: null,
        text: {

            value: `${(EnderecosUtilizados / EnderecosTotais * 100).toFixed(2)}%`,
                       
        }
    });

    const barPedidos = new ProgressBar.Circle(containerPedidos, {
        strokeWidth: 10, // Largura da barra
        easing: 'easeInOut',
        duration: 1400,
        color: 'rgb(17, 45, 126)', // Cor da barra
        trailColor: 'rgb(93, 140, 233)', // Cor do fundo da barra
        trailWidth: 10, // Largura do fundo da barra
        svgStyle: null,
        text: {

            value: `${(PedidosCompletos / PedidosRetorna * 100).toFixed(2)}%`,
           
            
        }
    });

    const percentualAtingimento = (RealizadoApi / MetaApi);
    const percentualAtingimentoReposicao = (PecasRepostasApi / PecasFase);
    const percentualAtingimentoEnderecos = (EnderecosUtilizados / EnderecosTotais);
    const percentualAtingimentoPedidos = (PedidosCompletos / PedidosRetorna);

    bar.animate(percentualAtingimento); // Anima a barra para o percentual de atingimento
    barReposicao.animate(percentualAtingimentoReposicao); // Anima a barra para o percentual de atingimento
    barEnderecos.animate(percentualAtingimentoEnderecos); // Anima a barra para o percentual de atingimento
    barPedidos.animate(percentualAtingimentoPedidos); // Anima a barra para o percentual de atingimento
    
}

window.addEventListener('load', async () => {
    await ChamadaApiEnderecos(ApiEnderecos, 1, 5),ChamadaApiPedidos(ApiPedidos),ChamadaApi(Api, CriarGrafico);
});
