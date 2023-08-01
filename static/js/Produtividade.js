function criarTabelasProdutividade(listaDados, tabela) {
    const tabelaProdutividade = document.getElementById(tabela);
    tabelaProdutividade.innerHTML = '';
    
  
    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const ColunaRanking = document.createElement('th');
    const ColunaColaborador = document.createElement('th');
    const ColunaQuantidade = document.createElement('th');
    const ColunaRitmo = document.createElement('th');
  
    ColunaRanking.textContent = 'Ranking';
    ColunaColaborador.textContent = 'Colaborador';
    ColunaQuantidade.textContent = 'Quantidade';
    ColunaRitmo.textContent = 'Ritmo';
  
    cabecalhoRow.appendChild(ColunaRanking);
    cabecalhoRow.appendChild(ColunaColaborador);
    cabecalhoRow.appendChild(ColunaQuantidade);
    cabecalhoRow.appendChild(ColunaRitmo);
    cabecalho.appendChild(cabecalhoRow);
    tabelaProdutividade.appendChild(cabecalho);
  
    // Ordena a lista de dados pela quantidade em ordem decrescente
    listaDados.sort((a, b) => b.qtde - a.qtde);
  
    listaDados.forEach((item, index) => {
      const row = document.createElement('tr');
      const ColunaRanking = document.createElement('td');
      const ColunaColaborador = document.createElement('td');
      const ColunaQuantidade = document.createElement('td');
      const ColunaRitmo = document.createElement('td');
  
      ColunaRanking.textContent = `${index + 1}º`; // Define o ranking baseado no índice
      ColunaColaborador.textContent = item.nome;
      ColunaQuantidade.textContent = item.qtde;
      ColunaRitmo.textContent = item.ritmo; // Preencha com o valor adequado
  
      row.appendChild(ColunaRanking);
      row.appendChild(ColunaColaborador);
      row.appendChild(ColunaQuantidade);
      row.appendChild(ColunaRitmo);
  
      tabelaProdutividade.appendChild(row);
    });
  }
  



function tabelaProdutividade (Consulta, Tabela, dataIni, DataFim){
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
                    throw new Error('Erro ao atribuir pedidos');
                }
            })
            .then(data => {
                console.log(data);
                criarTabelasProdutividade(data, Tabela);
                
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

})

      
      window.addEventListener('load', () => {
        const currentDate = new Date();
        const formattedDate = getFormattedDate(currentDate);
      
        tabelaProdutividade("TagsReposicao", "TAbelaRepositor", formattedDate, formattedDate);
        tabelaProdutividade("TagsSeparacao", "TAbelaSeparador", formattedDate, formattedDate);

        document.getElementById("DataInicial").value = formattedDate
        document.getElementById("DataFinal").value = formattedDate
        setTimeout(atualizarTabelas, 10000); // 60000 ms = 1 minuto
    });
      

    function atualizarTabelas() {
        const DataIni = document.getElementById("DataInicial").value;
        const DataFin = document.getElementById("DataFinal").value;

        console.log(DataIni);
        console.log(DataFin);
      
        Promise.all([
          tabelaProdutividade("TagsReposicao", "TAbelaRepositor", DataIni, DataFin),
          tabelaProdutividade("TagsSeparacao", "TAbelaSeparador", DataIni, DataFin)
        ]).then(() => {
          criarTabelasProdutividade(dataReposicao, "TAbelaRepositor");
          criarTabelasProdutividade(dataSeparacao, "TAbelaSeparador");
      
          // Agendar a próxima atualização após 1 minuto
          setTimeout(atualizarTabelas, 60000);
        }).catch(error => {
          console.error(error);
          // Em caso de erro, agendar a próxima atualização após 1 minuto
          setTimeout(atualizarTabelas, 60000);
        });
      }

      
