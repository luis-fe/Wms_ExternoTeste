
let qtdSugerida = 0;
let valorSugerido = 0;
let qtdSugerida1 = 0;
let valorSugerido1 = 0;

function criarTabelasProdutividade(listaDados, tabela, exibirColunas = false) {
    const tabelaProdutividade = document.getElementById(tabela);
    tabelaProdutividade.innerHTML = '';
    
  
    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const ColunaRanking = document.createElement('th');
    const ColunaColaborador = document.createElement('th');
    const ColunaQuantidade = document.createElement('th');
    const ColunaQuantidadePedidos = document.createElement('th');
    const ColunaMediaPedidos = document.createElement('th');
    const ColunaRitmo = document.createElement('th');
  
    ColunaRanking.textContent = 'Ranking';
    ColunaColaborador.textContent = 'Colaborador';
    ColunaQuantidade.textContent = 'Quantidade';
    ColunaQuantidadePedidos.textContent = 'Quantidade Pedidos';
    ColunaMediaPedidos.textContent = 'Média de Pçs';
    ColunaRitmo.textContent = 'Ritmo';

  
    cabecalhoRow.appendChild(ColunaRanking);
    cabecalhoRow.appendChild(ColunaColaborador);
    cabecalhoRow.appendChild(ColunaQuantidade);
    if (exibirColunas === true) { // Verifica se a coluna extra deve ser exibida
      cabecalhoRow.appendChild(ColunaQuantidadePedidos);
      cabecalhoRow.appendChild(ColunaMediaPedidos);
    }
    cabecalhoRow.appendChild(ColunaRitmo);
    cabecalho.appendChild(cabecalhoRow);
    tabelaProdutividade.appendChild(cabecalho);
  
    // Ordena a lista de dados pela quantidade em ordem decrescente
    //listaDados.sort((a, b) => b.qtde - a.qtde);
  
    listaDados.forEach((item, index) => {
      const row = document.createElement('tr');
      const ColunaRanking = document.createElement('td');
      const ColunaColaborador = document.createElement('td');
      const ColunaQuantidade = document.createElement('td');
      const ColunaQuantidadePedidos = document.createElement('td');
      const ColunaMediaPedidos = document.createElement('td');
      const ColunaRitmo = document.createElement('td');
  
      ColunaRanking.textContent = `${index + 1}º`; // Define o ranking baseado no índice
      ColunaColaborador.textContent = item.nome;
      ColunaQuantidade.textContent = item.qtde;
      ColunaQuantidadePedidos.textContent = item["Qtd Pedido"];
      ColunaMediaPedidos.textContent = item["Méd pçs/ped."];
      ColunaRitmo.textContent = item.ritmo; // Preencha com o valor adequado
  
      row.appendChild(ColunaRanking);
      row.appendChild(ColunaColaborador);
      row.appendChild(ColunaQuantidade);
      if (exibirColunas === true) { // Verifica se a coluna extra deve ser exibida
        row.appendChild(ColunaQuantidadePedidos);
        row.appendChild(ColunaMediaPedidos);
      }
      row.appendChild(ColunaRitmo);
  
      tabelaProdutividade.appendChild(row);
    });
  }
  
  function formatarMoeda(valor) {
    const formatoMoeda = {
      style: "currency",
      currency: "BRL"
    };
        return valor.toLocaleString("pt-BR", formatoMoeda);}

const ApiDistribuicao = 'http://192.168.0.183:5000/api/FilaPedidos';

function DadosRetorna (){
    fetch(ApiDistribuicao, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'a40016aabcx9'
        },
      })
        .then(response => {
          if (response.ok) {
            return response.json();
          } else {
            alert("Erro na Atualização, Recarregue a página!\nSe o problema persistir, contate o Administrador!");
          }
        })
        .then(data => {
            
          const TipoNotaFiltrado = data.filter(item => item["03-TipoNota"] !== "39 - BN MPLUS");
          const Filtro2 = TipoNotaFiltrado.filter(item => item["22- situacaopedido"] !== "Em Conferencia");
          Filtro2.forEach(item => {
            item["12-vlrsugestao"] = item["12-vlrsugestao"].replace("R$", "");
            item["12-vlrsugestao"] = parseFloat(item["12-vlrsugestao"]).toFixed(2);
    
            qtdSugerida += item["15-qtdesugerida"];
            valorSugerido += parseFloat(item["12-vlrsugestao"]);
        

        });

            const TipoNotaFiltrado1 = data.filter(item2 => item2["03-TipoNota"] === "39 - BN MPLUS");
            TipoNotaFiltrado1.forEach(item2 => {
                item2["12-vlrsugestao"] = item2["12-vlrsugestao"].replace("R$", "");
                item2["12-vlrsugestao"] = parseFloat(item2["12-vlrsugestao"]).toFixed(2);
                qtdSugerida1 += item2["15-qtdesugerida"];
                valorSugerido1 += parseFloat(item2["12-vlrsugestao"]);
            
          });
    
          document.getElementById("RetornaPcs").textContent = qtdSugerida;
          document.getElementById("RetornaValor").textContent = formatarMoeda(valorSugerido); // Formata o valor após a soma
          document.getElementById("RetornaMplus").textContent = qtdSugerida1;
          document.getElementById("RetornaMplusR$").textContent = formatarMoeda(valorSugerido1); // Formata o valor após a soma
          qtdSugerida = 0;
          valorSugerido = 0;
          qtdSugerida1 = 0;
          valorSugerido1 = 0;
    
        })
        .catch(error => {
          console.error(error);
        });
    }


    function getFormattedDate(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
      }



function tabelaProdutividade (Consulta, Tabela, dataIni, DataFim, NomeRecorde, QtdRecorde, Qtd, situacaoColuna){
    fetch (`http://192.168.0.183:5000/api/${Consulta}/Resumo?DataInicial=${dataIni}&DataFinal==${DataFim}`,{
        method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'a40016aabcx9'
            },
        })
            .then(response => {
                if (response.ok) {
                    return response.json();
                    
                } else {
                    alert("Erro na Atualização, Recarregue a página!\nSe o problema persistir contacte o Administrador!")
                }
            })
            .then(data => {
                const detalhamentoRanking = data[0]['3- Ranking Repositores'];
                const detalhamentoNomeRecord = data[0]["1- Record Repositor"];
                const detalhamentoQtdRecord = data[0]["1.1- Record qtd"];
                const Total = data[0]["2 Total Periodo"];
                criarTabelasProdutividade(detalhamentoRanking, Tabela, situacaoColuna);
                document.getElementById(NomeRecorde).textContent = detalhamentoNomeRecord;
                document.getElementById(QtdRecorde).textContent = `${detalhamentoQtdRecord} Pçs`
                document.getElementById(Qtd).textContent = `${parseFloat(Total)} Pçs`;
                
            })
            .catch(error => {
                console.error(error);
            });
    }


    function getFormattedDate(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
      }

const botaoFiltrar = document.getElementById("btnFiltrar")


botaoFiltrar.addEventListener('click', () => {
    atualizarTabelas();
    document.getElementById("btnFiltrar").disabled = true;
})

      
      window.addEventListener('load', () => {
        const currentDate = new Date();
        const formattedDate = getFormattedDate(currentDate);
      
        tabelaProdutividade("TagsReposicao", "TAbelaRepositor", formattedDate, formattedDate, "NomeRecordeRep", "ValorRecordeRep", "LabelTotalReposto", false);
        tabelaProdutividade("TagsSeparacao", "TAbelaSeparador", formattedDate, formattedDate, "NomeRecordeSep", "ValorRecordeSep", "LabelTotalSeparado", true);
        DadosRetorna();

        document.getElementById("DataInicial").value = formattedDate
        document.getElementById("DataFinal").value = formattedDate
        setTimeout(atualizarTabelas, 10000); // 60000 ms = 1 minuto
    });
      

    function atualizarTabelas() {
        const DataIni = document.getElementById("DataInicial").value;
        const DataFin = document.getElementById("DataFinal").value;
      
        Promise.all([
          tabelaProdutividade("TagsReposicao", "TAbelaRepositor", DataIni, DataFin, "NomeRecordeRep", "ValorRecordeRep", "LabelTotalReposto", false),
          tabelaProdutividade("TagsSeparacao", "TAbelaSeparador", DataIni, DataFin, "NomeRecordeSep", "ValorRecordeSep", "LabelTotalSeparado", true)
        ]).then(() => {
          criarTabelasProdutividade(dataReposicao, "TAbelaRepositor", false);
          criarTabelasProdutividade(dataSeparacao, "TAbelaSeparador", true);
          document.getElementById("btnFiltrar").disabled = false;
        }).catch(error => {
          console.error(error);
          document.getElementById("btnFiltrar").disabled = false; // Habilita o botão mesmo em caso de erro
        });
      
        DadosRetorna(); // Chama a função DadosRetorna() para atualizar os dados
        setTimeout(atualizarTabelas, 60000); // 60000 ms = 1 minuto
      }

      
