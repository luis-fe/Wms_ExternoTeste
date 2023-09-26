const ApiConsultaChamados = "http://192.168.0.183:5000/api/chamados";
const ApiConsultaAreaChamados = "http://192.168.0.183:5000/api/area"
const ApiNovoChamado = "http://192.168.0.183:5000/api/NovoChamado";
const ApiImagem = "http://192.168.0.183:5000/api/upload"
const Token = "a40016aabcx9";
let IdChamado = ""
let dadosApi = "";
let UsuarioSelecionadoTabela;
let NomeSelecionadoTabela;
let FuncaoSelecionadoTabela;
let SituacaoSelecionadoTabela;

async function ChamadaApi(api){
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
            dadosApi = data;
        } else {
            throw new Error('Erro No Retorno');
        }
    } catch (error) {
        console.error(error);
    }
};

function criarTabelaChamados(listaChamados) {
    const tabelaChamados = document.getElementById('TabelaChamados');
    tabelaChamados.innerHTML = ''; // Limpa o conteúdo da tabela antes de preenchê-la novamente

    // Cria o cabeçalho da tabela
    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const colunaCheckbox = document.createElement('th');
    const ColunaId = document.createElement('th');
    const ColunaDescricao = document.createElement('th');
    const ColunaSolicitante = document.createElement('th');
    const ColunaInicioChamado = document.createElement('th');
    const ColunaFinalChamado = document.createElement('th');
    const ColunaTipoChamado = document.createElement('th');
    const StatusChamado = document.createElement('th');

    
    colunaCheckbox.textContent = '';
    ColunaId.textContent = 'Id';
    ColunaDescricao.textContent = 'Descrição';
    ColunaSolicitante.textContent = 'Solicitante';
    ColunaInicioChamado.textContent = 'Data Chamado';
    ColunaFinalChamado.textContent = 'Data Finalização';
    ColunaTipoChamado.textContent = 'Tipo';
    StatusChamado.textContent = 'Status';

    colunaCheckbox.style.width = '40px';
    ColunaId.style.width = '100px';
    ColunaDescricao.style.width = '600px';

    cabecalhoRow.appendChild(colunaCheckbox);
    cabecalhoRow.appendChild(ColunaId);
    cabecalhoRow.appendChild(ColunaDescricao);
    cabecalhoRow.appendChild(ColunaSolicitante);
    cabecalhoRow.appendChild(ColunaInicioChamado);
    cabecalhoRow.appendChild(ColunaFinalChamado);
    cabecalhoRow.appendChild(ColunaTipoChamado);
    cabecalhoRow.appendChild(StatusChamado);
    cabecalho.appendChild(cabecalhoRow);
    tabelaChamados.appendChild(cabecalho);


    listaChamados.forEach(item => {
        const row = document.createElement('tr');
        const colunaCheckbox = document.createElement('td');
        const ColunaId = document.createElement('td');
        const ColunaDescricao = document.createElement('td');
        const ColunaSolicitante = document.createElement('td');
        const ColunaInicioChamado = document.createElement('td');
        const ColunaFinalChamado = document.createElement('td');
        const ColunaTipoChamado = document.createElement('td');
        const StatusChamado = document.createElement('td');


        const checkboxUsuarios = document.createElement('input');
        checkboxUsuarios.type = 'checkbox'; // Alterado de 'radio' para 'checkbox'
        checkboxUsuarios.value = item["id_chamado"];
        checkboxUsuarios.name = 'checkboxUsuarios';
        checkboxUsuarios.addEventListener('change', function(event) {
            const checkboxes = document.getElementsByName('checkboxUsuarios');
            checkboxes.forEach(box => {
              if (box !== event.target) {
                box.checked = false;
              }
            });
          });
        colunaCheckbox.appendChild(checkboxUsuarios);
        ColunaId.textContent = item["id_chamado"];
        ColunaDescricao.textContent = item["descricao_chamado"];
        ColunaSolicitante.textContent = item["solicitante"];
        ColunaInicioChamado.textContent = item["data_chamado"];
        ColunaFinalChamado.textContent = item["data_finalizacao_chamado"];
        ColunaTipoChamado.textContent = item["tipo_chamado"];
        StatusChamado.textContent = item["status_chamado"];

    
        row.appendChild(colunaCheckbox);
        row.appendChild(ColunaId);
        row.appendChild(ColunaDescricao);
        row.appendChild(ColunaSolicitante);
        row.appendChild(ColunaInicioChamado);
        row.appendChild(ColunaFinalChamado);
        row.appendChild(ColunaTipoChamado);
        row.appendChild(StatusChamado);
        tabelaChamados.appendChild(row);
        });

    }

const modalLoading = document.getElementById("ModalLoading");


function AbrirModalLoading() {
    modalLoading.style.display = "block";
}

function FecharModalLoading() {
    modalLoading.style.display = "none";
}



const fileInput = document.getElementById("fileInput");
const addImagemIcon = document.getElementById("AddImagem");
const imagePreview = document.getElementById("imagePreview");
const deleteimagem = document.getElementById("DeleteImagem")



addImagemIcon.addEventListener("click", function() {
  fileInput.click();
});



fileInput.addEventListener("change", function() {
  const selectedFile = fileInput.files[0];

  if (selectedFile) {
    const imageUrl = URL.createObjectURL(selectedFile);
    imagePreview.src = imageUrl;
    imagePreview.style.display = "block";
    deleteimagem.style.display = "flex"
    
  }
});

deleteimagem.addEventListener("click", function() {
    fileInput.value = ""; // Limpa a seleção do arquivo
    imagePreview.src = ""; // Limpa a visualização da imagem
    deleteimagem.style.display = "none"
  });


    async function SalvarChamados(api){
const nomeUsuario = localStorage.getItem('nomeUsuario');
const dataAtual = new Date();
const ano = dataAtual.getFullYear().toString();
const mes = (dataAtual.getMonth() + 1).toString().padStart(2, '0'); // +1 porque os meses são baseados em zero
const dia = dataAtual.getDate().toString().padStart(2, '0');
const dataFormatada = `${ano}/${mes}/${dia}`;

console.log(dataFormatada);

        console.log(nomeUsuario)
        const Salvar = {
            "solicitante": nomeUsuario,
            "data_chamado": dataFormatada,
            "tipo_chamado": document.getElementById('OpcoesTipoChamado').value,
            "area": document.getElementById('OpcoesArea').value,
            "descricao_chamado": document.getElementById('InputDescricaoChamado').value
        }
        try {
            const response = await fetch(api, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': Token
                },
                body: JSON.stringify(Salvar),
            });
        
            if (response.ok) {
                const data = await response.json();
                const resultado = data["id_chamado"]
                console.log(resultado)
                IdChamado = resultado
            } else {
                throw new Error('Erro No Retorno');
            }
        } catch (error) {
            console.error(error);
        }
    };



const BotaoSalvarNovoChamado = document.getElementById("BotaoSalvarChamado");

BotaoSalvarNovoChamado.addEventListener('click', async () => {
    await SalvarChamados(ApiNovoChamado);
    EnviarImagemParaAPI(ApiImagem)
    
});




window.addEventListener('load', async () => { await ChamadaApi(ApiConsultaChamados); criarTabelaChamados(dadosApi)});



const InputBuscaChamados = document.getElementById('InputBuscaChamados');
const TabelaChamados = document.getElementById('TabelaChamados');
        
InputBuscaChamados.addEventListener('keyup', () => {
    const ExpressaoChamados = InputBuscaChamados.value.trim().toLowerCase();
    const LinhasTabelasChamados = TabelaChamados.getElementsByTagName('tr');
        
        for (let i = 1; i < LinhasTabelasChamados.length; i++) {
            const LinhasChamados = LinhasTabelasChamados[i];
            const ColunasChamados = LinhasChamados.getElementsByTagName('td');
            let EncontrouChamados = false;
        
        for (let j = 1; j < ColunasChamados.length; j++) {
            const ConteudoColunasChamados = ColunasChamados[j].textContent.trim().toLowerCase();
        
        if (ConteudoColunasChamados.includes(ExpressaoChamados)) {
            EncontrouChamados = true;
            break;
            }
        }
        
        if (EncontrouChamados) {
            LinhasChamados.style.display = '';
        } else {
            LinhasChamados.style.display = 'none';
        }
        }
    });

    async function EnviarImagemParaAPI(api) {
        // Obtenha o elemento de input do arquivo
        const fileInput = document.getElementById("fileInput");
        
        // Verifique se um arquivo foi selecionado
        if (fileInput.files.length > 0) {
            const formData = new FormData();
            formData.append('file', fileInput.files[0]); // 'file' deve corresponder à chave esperada pelo backend
    
    
            try {
                const response = await fetch(`${api}/${IdChamado}`, {
                    method: 'POST',
                    headers: {
                        'Authorization': Token,
                    },
                    body: formData,
                });
    
                if (response.ok) {
                    const data = await response.json();
                    console.log('Imagem enviada com sucesso:', data);
                    // Aqui, você pode continuar com o código para salvar o chamado após o envio da imagem, se necessário.
                } else {
                    console.error('Erro ao enviar imagem para a API.');
                }
            } catch (error) {
                console.error(error);
            }
        } else {
            console.error('Nenhuma imagem selecionada.');
        }
    }
    
    
   


const BotaoNovoChamado = document.getElementById("AdicionarChamado");
const AbrirModalNovoChamado= document.getElementById("ModalNovoChamado");
const FecharModalNovoUsuario = document.getElementById("FecharModalNovoChamado");

BotaoNovoChamado.addEventListener("click", async () => {
    AbrirModalNovoChamado.style.display = "block";
    await ChamadaApi(ApiConsultaAreaChamados);
  
    // Preencha o elemento 'OpcoesArea' com os dados da API
    const OpcoesArea = document.getElementById("OpcoesArea");
    OpcoesArea.innerHTML = "";
  
    // Adicione a primeira opção vazia
    const optionVazia = document.createElement("option");
    optionVazia.value = "";
    optionVazia.textContent = ""; // Você pode definir o texto desejado aqui
    OpcoesArea.appendChild(optionVazia);
  
    // Verifique se 'dadosApi' não está vazio
    if (dadosApi && Array.isArray(dadosApi)) {
      dadosApi.forEach(area => {
        const option = document.createElement("option");
        option.value = area.area;
        option.textContent = area.area;
        OpcoesArea.appendChild(option);
      });
    } else {
      console.error("Dados da área não encontrados na API.");
    };


});
  


FecharModalNovoUsuario.addEventListener('click', () => {
    document.getElementById("InputDescricaoChamado").value = ""
    document.getElementById("OpcoesTipoChamado").value = ""
    document.getElementById("OpcoesArea").value = ""
    AbrirModalNovoChamado.style.display = "none";

    
    
});






//**************************************************************************************************************************************************************************** */




function CheckboxSelecionada() {
    const tabela = document.getElementById('TabelaUsuarios1');
    const linhas = tabela.getElementsByTagName('tr');

    for (let i = 1; i < linhas.length; i++) {
        const linha = linhas[i];
        const checkbox = linha.querySelector('input[type="checkbox"]:checked');

        if (checkbox) {
            const colunas = linha.getElementsByTagName('td');
            UsuarioSelecionadoTabela = colunas[1].textContent.trim();
            NomeSelecionadoTabela = colunas[2].textContent.trim();
            FuncaoSelecionadoTabela = colunas[3].textContent.trim();
            SituacaoSelecionadoTabela = colunas[4].textContent.trim();
            return UsuarioSelecionadoTabela; // Retorna o valor da variável
        } else {
            UsuarioSelecionadoTabela = "";
            NomeSelecionadoTabela = "";
            FuncaoSelecionadoTabela = "";
            SituacaoSelecionadoTabela = "";
        }
    }

    return null; // Retorna null se nenhuma linha estiver selecionada
}
