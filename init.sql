-- Criando o Schema chamado "Reposicao"
CREATE SCHEMA "Reposicao" AUTHORIZATION postgres;
-- Criando as Funcoes desse schema
-- Funcao 1: atualizar_status
CREATE OR REPLACE FUNCTION "Reposicao".atualizar_status()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    -- Atualizar o valor da coluna "status" após um update ou insert
    NEW.status := CONCAT(CAST(NEW.qtdesugerida - NEW.necessidade AS TEXT), '/', CAST(NEW.qtdesugerida AS TEXT));
    RETURN NEW;
END;
$function$
;
--Funcao 2: backup_and_delete_tagsreposicao
CREATE OR REPLACE FUNCTION "Reposicao".backup_and_delete_tagsreposicao()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    -- Copiar os dados da tabela original para a tabela de backup
    INSERT INTO "Reposicao".tagsreposicao_inventario
    SELECT *
    FROM "Reposicao".tagsreposicao
    WHERE "Endereco" = NEW.endereco; -- NEW.endereco é o valor inserido na tabela "Reposicao".registroinventario
    -- Excluir as linhas correspondentes na tabela original
    DELETE FROM "Reposicao".tagsreposicao
    WHERE "Endereco" = NEW.endereco; -- NEW.endereco é o valor inserido na tabela "Reposicao".registroinventario
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION "Reposicao".getdatageracao()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    datageracao_value text;
	BEGIN
        -- Recupera o valor de datageracao para o codpedido do NEW
        SELECT datageracao INTO datageracao_value
        FROM "Reposicao".filaseparacaopedidos
        WHERE codigopedido = NEW.codpedido;

        -- Atualiza o valor de datageracao na tabela tagsreposicao
        UPDATE "Reposicao".finalizacao_pedido
        SET datageracao = datageracao_value
        WHERE codpedido = NEW.codpedido;
    END;$function$
;

CREATE OR REPLACE FUNCTION "Reposicao".updatereposicao()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    numeroop_value text;
    totalop_value text;
    tamanho_value text;
    epc_value text;
    descricao_value text;
    cor_value text;
    usuario_value text;
    engenharia_value text;
    codreduzido_value text;
	codnaturezaatual_value text;
	resticao_value text;
BEGIN
    SELECT numeroop, totalop, tamanho, epc, descricao, cor, usuario, engenharia, codreduzido, codnaturezaatual,resticao INTO numeroop_value, totalop_value, tamanho_value, epc_value, descricao_value, cor_value,
     usuario_value, engenharia_value, codreduzido_value, codnaturezaatual_value, resticao_value
    FROM "Reposicao".filareposicaoportag
    WHERE codbarrastag = NEW.codbarrastag;
    -- Verificar se usuario_value é NULL antes de executar o UPDATE
    IF codreduzido_value IS NOT NULL THEN
        -- Salvar os valores recuperados nas colunas correspondentes na tabela tagsreposicao
        UPDATE "Reposicao".tagsreposicao
        SET numeroop = numeroop_value, totalop = totalop_value, tamanho = tamanho_value, epc = epc_value, descricao = descricao_value, cor = cor_value,
        engenharia = engenharia_value, codreduzido = codreduzido_value, natureza = codnaturezaatual_value, resticao = resticao_value
        WHERE codbarrastag = NEW.codbarrastag;
    END IF;
    DELETE FROM "Reposicao".filareposicaoportag
    WHERE codbarrastag = NEW.codbarrastag;

    RETURN NEW;
	END;
$function$
;

CREATE OR REPLACE FUNCTION "Reposicao".updateseparacao()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    numeroop_value text;
    totalop_value text;
    tamanho_value text;
    epc_value text;
   	descricao_value text;
   	cor_value text;
   	usuario_value text;
   	engenharia_value text;
   	codreduzido_value text;
BEGIN
    SELECT numeroop, totalop, tamanho, epc, descricao, cor, usuario, engenharia, codreduzido INTO numeroop_value, totalop_value, tamanho_value, epc_value, descricao_value, cor_value,
     usuario_value, engenharia_value, codreduzido_value
    FROM "Reposicao".tagsreposicao
    WHERE codbarrastag = NEW.codbarrastag;
    -- Salvar os valores recuperados nas colunas correspondentes na tabela tagsreposicao
    UPDATE "Reposicao".tags_separacao
    SET numeroop = numeroop_value, totalop = totalop_value, tamanho = tamanho_value, epc = epc_value, descricao = descricao_value, cor = cor_value,
     usuario_rep = usuario_value, engenharia = engenharia_value, codreduzido = codreduzido_value
    WHERE codbarrastag = NEW.codbarrastag;
   --
       DELETE FROM "Reposicao".tagsreposicao
    WHERE codbarrastag = NEW.codbarrastag;
    RETURN NEW;
END;
$function$
;

CREATE TABLE "Reposicao"."OPSDefeitoTecidos" (
	coditem text NULL,
	"nomeItem" text NULL,
	"nomeFornecedor" text NULL,
	repeticoessku int8 NULL,
	categoria text NULL,
	"numOPConfec" text NULL,
	"codRequisicao" float8 NULL,
	"qtdeEntregue" float8 NULL,
	"repeticaoOP" int8 NULL
);


-- "Reposicao"."OpsEstamparia" definition

-- Drop table

-- DROP TABLE "Reposicao"."OpsEstamparia";

CREATE TABLE "Reposicao"."OpsEstamparia" (
	"OPpai" text NULL,
	"codFase" int8 NULL,
	"nomeFaccionista" text NULL
);


-- "Reposicao"."SubstitutosSkuOP" definition

-- Drop table

-- DROP TABLE "Reposicao"."SubstitutosSkuOP";

CREATE TABLE "Reposicao"."SubstitutosSkuOP" (
	requisicao int8 NULL,
	numeroop text NULL,
	codproduto text NULL,
	databaixa_req text NULL,
	componente text NULL,
	nomecompontente text NULL,
	subst text NULL,
	nomesub text NULL,
	"coodigoSubs" text NULL,
	"coodigoPrincipal" text NULL,
	"codSortimento" text NULL,
	aplicacao text NULL,
	tipo text NULL,
	cor text NULL,
	"aplicacaoPad" text NULL,
	categoria text NULL,
	exessao text NULL,
	considera text NULL,
	id text NULL
);


-- "Reposicao".cadendereco definition

-- Drop table

-- DROP TABLE "Reposicao".cadendereco;

CREATE TABLE "Reposicao".cadendereco (
	codendereco text NOT NULL,
	rua text NULL,
	modulo text NULL,
	posicao text NULL,
	tipo text NULL,
	codempresa varchar NULL,
	natureza varchar NULL,
	endereco_subst varchar NULL,
	restrito varchar NULL,
	pre_reserva varchar NULL,
	reservado varchar NULL,
	CONSTRAINT cadendereco_pkey PRIMARY KEY (codendereco)
);


-- "Reposicao".cadusuarios definition

-- Drop table

-- DROP TABLE "Reposicao".cadusuarios;

CREATE TABLE "Reposicao".cadusuarios (
	codigo int4 NOT NULL,
	nome varchar NULL,
	senha varchar NULL,
	situacao varchar NULL,
	funcao varchar NULL,
	empresa varchar NULL,
	CONSTRAINT cadusuarios_pkey PRIMARY KEY (codigo)
);


-- "Reposicao".caixas definition

-- Drop table

-- DROP TABLE "Reposicao".caixas;

CREATE TABLE "Reposicao".caixas (
	codcaixa varchar NOT NULL,
	nomecaixa varchar NULL,
	tamanhocaixa varchar NULL
);


-- "Reposicao".comunicaoskucsw definition

-- Drop table

-- DROP TABLE "Reposicao".comunicaoskucsw;

CREATE TABLE "Reposicao".comunicaoskucsw (
	codpedido text NULL,
	produto text NULL,
	qtdesugerida float8 NULL,
	qtdepecasconf float8 NULL,
	valorunitarioliq float8 NULL,
	necessidade float8 NULL,
	datahora text NULL,
	reservado text NULL,
	endereco text NULL
);


-- "Reposicao"."configuracaoTipo" definition

-- Drop table

-- DROP TABLE "Reposicao"."configuracaoTipo";

CREATE TABLE "Reposicao"."configuracaoTipo" (
	tipo varchar NOT NULL,
	CONSTRAINT "configuracaoTipo_pkey" PRIMARY KEY (tipo)
);


-- "Reposicao".configuracoes definition

-- Drop table

-- DROP TABLE "Reposicao".configuracoes;

CREATE TABLE "Reposicao".configuracoes (
	empresa varchar NOT NULL,
	natureza varchar NULL,
	nomeempresa varchar NULL,
	id serial4 NOT NULL,
	CONSTRAINT configuracoes_pkey PRIMARY KEY (id)
);


-- "Reposicao".conftiponotacsw definition

-- Drop table

-- DROP TABLE "Reposicao".conftiponotacsw;

CREATE TABLE "Reposicao".conftiponotacsw (
	empresa varchar NULL,
	tiponota varchar NULL,
	desc_tipo_nota varchar NULL
);


-- "Reposicao".filareposicaoof definition

-- Drop table

-- DROP TABLE "Reposicao".filareposicaoof;

CREATE TABLE "Reposicao".filareposicaoof (
	codbarrastag text NULL,
	codreduzido text NULL,
	engenharia text NULL,
	descricao text NULL,
	situacao int8 NULL,
	natureza text NULL,
	codempresa int8 NULL,
	cor text NULL,
	tamanho text NULL,
	numeroop text NULL
);


-- "Reposicao".filareposicaoportag definition

-- Drop table

-- DROP TABLE "Reposicao".filareposicaoportag;

CREATE TABLE "Reposicao".filareposicaoportag (
	codbarrastag varchar NOT NULL,
	codnaturezaatual varchar NULL,
	engenharia varchar NULL,
	codreduzido varchar NULL,
	descricao varchar NULL,
	numeroop varchar NULL,
	cor varchar NULL,
	tamanho varchar NULL,
	usuario varchar NULL,
	"Situacao" varchar NULL,
	epc varchar NULL,
	"DataHora" varchar NULL,
	totalop varchar NULL,
	dataentrada varchar NULL,
	codempresa varchar NULL,
	resticao varchar NULL,
	considera varchar NULL
);


-- "Reposicao".filaseparacaopedidos definition

-- Drop table

-- DROP TABLE "Reposicao".filaseparacaopedidos;

CREATE TABLE "Reposicao".filaseparacaopedidos (
	codigopedido varchar NOT NULL,
	vlrsugestao varchar NULL,
	datageracao varchar NULL,
	situacaosugestao varchar NULL,
	datafaturamentoprevisto varchar NULL,
	priorizar varchar NULL,
	datageracaotagfila varchar NULL,
	codcliente varchar NULL,
	codrepresentante varchar NULL,
	codtiponota varchar NULL,
	datahora varchar NULL,
	desc_cliente varchar NULL,
	cod_usuario varchar NULL,
	desc_representante varchar NULL,
	desc_tiponota varchar NULL,
	contagem varchar NULL,
	agrupamentopedido varchar NULL,
	cidade varchar NULL,
	estado varchar NULL,
	condvenda varchar NULL,
	condicaopgto varchar NULL,
	situacaopedido varchar NULL,
	prioridade varchar NULL,
	baixacsw varchar NULL
);


-- "Reposicao".finalizacao_pedido definition

-- Drop table

-- DROP TABLE "Reposicao".finalizacao_pedido;

CREATE TABLE "Reposicao".finalizacao_pedido (
	codpedido varchar NOT NULL,
	datafinalizacao varchar NULL,
	usuario varchar NULL,
	"codCaixa" varchar NULL,
	"tamCaixa" varchar NULL,
	"dataInicio" varchar NULL,
	situacao varchar NULL,
	datageracao varchar NULL,
	dataatribuicao varchar NULL,
	vlrsugestao varchar NULL,
	qtdepçs varchar NULL,
	qtdcaixa int4 NULL,
	tamcaixa2 varchar NULL,
	qtdcaixa2 int4 NULL,
	tamcaixa3 varchar NULL,
	qtdcaixa3 int4 NULL,
	tamcaixa4 varchar NULL,
	qtdcaixa4 int4 NULL,
	baixacsw varchar NULL
);


-- "Reposicao".horariodolog definition

-- Drop table

-- DROP TABLE "Reposicao".horariodolog;

CREATE TABLE "Reposicao".horariodolog (
	codigo varchar NULL,
	datalog varchar NULL
);


-- "Reposicao".necessidadeendereco definition

-- Drop table

-- DROP TABLE "Reposicao".necessidadeendereco;

CREATE TABLE "Reposicao".necessidadeendereco (
	codpedido text NULL,
	produto text NULL,
	endereco text NULL,
	"Necessidade Endereco" int8 NULL
);


-- "Reposicao"."pedidosTransferecia" definition

-- Drop table

-- DROP TABLE "Reposicao"."pedidosTransferecia";

CREATE TABLE "Reposicao"."pedidosTransferecia" (
	codigopedido varchar NOT NULL,
	descricaopedido varchar NOT NULL,
	de varchar NOT NULL,
	para varchar NOT NULL,
	natureza varchar NOT NULL,
	situacao varchar NOT NULL,
	usuario varchar NOT NULL,
	datageracao varchar NULL,
	CONSTRAINT "pedidosTransferecia_pkey" PRIMARY KEY (codigopedido)
);


-- "Reposicao".pedidossku definition

-- Drop table

-- DROP TABLE "Reposicao".pedidossku;

CREATE TABLE "Reposicao".pedidossku (
	codpedido text NULL,
	produto text NULL,
	qtdesugerida float8 NULL,
	qtdepecasconf float8 NULL,
	endereco text NULL,
	necessidade float8 NULL,
	datahora text NULL,
	status text NULL,
	valorunitarioliq varchar NULL,
	reservado varchar NULL
);

-- Table Triggers

create trigger atualizar_status_trigger before
insert
    or
update
    on
    "Reposicao".pedidossku for each row execute procedure "Reposicao".atualizar_status();


-- "Reposicao".registroinventario definition

-- Drop table

-- DROP TABLE "Reposicao".registroinventario;

CREATE TABLE "Reposicao".registroinventario (
	usuario varchar NULL,
	"data" varchar NULL,
	endereco varchar NULL,
	situacao varchar NULL,
	datafinalizacao varchar NULL
);

-- Table Triggers

create trigger backup_and_delete_tagsreposicao after
insert
    on
    "Reposicao".registroinventario for each row execute procedure "Reposicao".backup_and_delete_tagsreposicao();


-- "Reposicao".saida_avulsa definition

-- Drop table

-- DROP TABLE "Reposicao".saida_avulsa;

CREATE TABLE "Reposicao".saida_avulsa (
	saida varchar NULL,
	codbarrastag varchar NULL,
	situacao varchar NULL,
	usuario varchar NULL,
	datareposicao varchar NULL,
	data_saida varchar NULL,
	situacao_csw varchar NULL,
	natureza_saida varchar NULL
);


-- "Reposicao".tags_separacao definition

-- Drop table

-- DROP TABLE "Reposicao".tags_separacao;

CREATE TABLE "Reposicao".tags_separacao (
	usuario text NULL,
	codbarrastag text NULL,
	codreduzido text NULL,
	"Endereco" text NULL,
	engenharia text NULL,
	"DataReposicao" text NULL,
	"EngenhariaPai" float8 NULL,
	descricao text NULL,
	epc text NULL,
	"StatusEndereco" text NULL,
	numeroop text NULL,
	cor text NULL,
	tamanho text NULL,
	totalop text NULL,
	codpedido text NULL,
	usuario_rep text NULL,
	dataseparacao text NULL,
	valorunitarioliq float8 NULL,
	ritmo int4 NULL,
	resticao varchar NULL
);


-- "Reposicao".tagsreposicao definition

-- Drop table

-- DROP TABLE "Reposicao".tagsreposicao;

CREATE TABLE "Reposicao".tagsreposicao (
	usuario text NULL,
	codbarrastag text NOT NULL,
	codreduzido text NULL,
	"Endereco" text NULL,
	engenharia text NULL,
	"DataReposicao" text NULL,
	"EngenhariaPai" text NULL,
	descricao text NULL,
	epc text NULL,
	"StatusEndereco" text NULL,
	numeroop text NULL,
	cor text NULL,
	tamanho text NULL,
	totalop text NULL,
	natureza text NULL,
	proveniencia text NULL,
	codempresa varchar NULL,
	usuario_inv varchar NULL,
	usuario_carga varchar NULL,
	datahora_carga text NULL,
	resticao varchar NULL,
	CONSTRAINT tagsreposicao_pkey PRIMARY KEY (codbarrastag)
);
CREATE INDEX fki_endereco ON "Reposicao".tagsreposicao USING btree ("Endereco");

-- Table Triggers

create trigger nova after
insert
    on
    "Reposicao".tagsreposicao for each row execute procedure "Reposicao".updatereposicao();


-- "Reposicao".tagsreposicao_inventario definition

-- Drop table

-- DROP TABLE "Reposicao".tagsreposicao_inventario;

CREATE TABLE "Reposicao".tagsreposicao_inventario (
	usuario text NULL,
	codbarrastag text NOT NULL,
	codreduzido text NULL,
	"Endereco" text NULL,
	engenharia text NULL,
	"DataReposicao" text NULL,
	"EngenhariaPai" text NULL,
	descricao text NULL,
	epc text NULL,
	"StatusEndereco" text NULL,
	numeroop text NULL,
	cor text NULL,
	tamanho text NULL,
	totalop text NULL,
	natureza varchar NULL,
	situacaoinventario text NULL,
	codempresa varchar NULL,
	usuario_inv varchar NULL,
	usuario_carga varchar NULL,
	datahora_carga varchar NULL,
	resticao varchar NULL
);

-- "Reposicao"."AtribuicaoDiariaPedidos" source

CREATE OR REPLACE VIEW "Reposicao"."AtribuicaoDiariaPedidos"
AS SELECT finalizacao_pedido.usuario
   FROM "Reposicao".finalizacao_pedido
  WHERE finalizacao_pedido.dataatribuicao::date = CURRENT_DATE;


-- "Reposicao"."ProducaoCargaEndereco" source

CREATE OR REPLACE VIEW "Reposicao"."ProducaoCargaEndereco"
AS SELECT dt.usuario,
    dt.datareposicao,
    dt.datatempo,
    dt.horario
   FROM ( SELECT tagsreposicao.usuario_carga AS usuario,
            tagsreposicao.datahora_carga::date AS datareposicao,
            tagsreposicao.datahora_carga::time without time zone AS horario,
            tagsreposicao.datahora_carga AS datatempo
           FROM "Reposicao".tagsreposicao
          WHERE tagsreposicao.usuario_carga IS NOT NULL) dt;


-- "Reposicao"."ProducaoRepositores" source

CREATE OR REPLACE VIEW "Reposicao"."ProducaoRepositores"
AS SELECT dt.usuario,
    dt.datareposicao,
    dt.datatempo,
    dt.horario
   FROM ( SELECT tagsreposicao.usuario,
            tagsreposicao."DataReposicao"::date AS datareposicao,
            tagsreposicao."DataReposicao"::time without time zone AS horario,
            tagsreposicao."DataReposicao" AS datatempo
           FROM "Reposicao".tagsreposicao
          WHERE tagsreposicao.usuario IS NOT NULL) dt;


-- "Reposicao"."ProducaoRepositores2" source

CREATE OR REPLACE VIEW "Reposicao"."ProducaoRepositores2"
AS SELECT dt.usuario,
    dt.datareposicao,
    dt.datatempo,
    dt.horario
   FROM ( SELECT tags_separacao.usuario_rep AS usuario,
            tags_separacao."DataReposicao"::date AS datareposicao,
            tags_separacao."DataReposicao" AS datatempo,
            tags_separacao."DataReposicao"::time without time zone AS horario
           FROM "Reposicao".tags_separacao
          WHERE tags_separacao.usuario_rep IS NOT NULL) dt;


-- "Reposicao"."ProducaoRepositores3" source

CREATE OR REPLACE VIEW "Reposicao"."ProducaoRepositores3"
AS SELECT dt.usuario,
    dt.datareposicao,
    dt.datatempo,
    dt.horario
   FROM ( SELECT reposicao_qualidade.usuario,
            reposicao_qualidade."DataReposicao"::date AS datareposicao,
            reposicao_qualidade."DataReposicao" AS datatempo,
            reposicao_qualidade."DataReposicao"::time without time zone AS horario
           FROM off.reposicao_qualidade
          WHERE reposicao_qualidade.usuario IS NOT NULL) dt;


-- "Reposicao"."ProducaoSeparadores" source

CREATE OR REPLACE VIEW "Reposicao"."ProducaoSeparadores"
AS SELECT dt.usuario,
    dt.dataseparacao,
    dt.ritmo,
    dt.codpedido,
    dt.datatempo,
    dt.tempo,
    dt.tempo AS horario
   FROM ( SELECT tags_separacao.usuario,
            tags_separacao.dataseparacao::date AS dataseparacao,
            tags_separacao.dataseparacao::time without time zone AS tempo,
            tags_separacao.ritmo,
            tags_separacao.codpedido,
            tags_separacao.dataseparacao AS datatempo
           FROM "Reposicao".tags_separacao
          WHERE tags_separacao.usuario IS NOT NULL) dt;


-- "Reposicao"."Tabela_Sku" source

CREATE OR REPLACE VIEW "Reposicao"."Tabela_Sku"
AS SELECT dt.engenharia,
    dt.codreduzido,
    dt.descricao,
    dt.tamanho,
    dt.cor
   FROM ( SELECT filareposicaoportag.engenharia,
            filareposicaoportag.codreduzido,
            filareposicaoportag.descricao,
            filareposicaoportag.tamanho,
            filareposicaoportag.cor
           FROM "Reposicao".filareposicaoportag
          GROUP BY filareposicaoportag.engenharia, filareposicaoportag.codreduzido, filareposicaoportag.descricao, filareposicaoportag.tamanho, filareposicaoportag.cor
        UNION
         SELECT tagsreposicao.engenharia,
            tagsreposicao.codreduzido,
            tagsreposicao.descricao,
            tagsreposicao.tamanho,
            tagsreposicao.cor
           FROM "Reposicao".tagsreposicao
          GROUP BY tagsreposicao.engenharia, tagsreposicao.codreduzido, tagsreposicao.descricao, tagsreposicao.tamanho, tagsreposicao.cor
        UNION
         SELECT tags_separacao.engenharia,
            tags_separacao.codreduzido,
            tags_separacao.descricao,
            tags_separacao.tamanho,
            tags_separacao.cor
           FROM "Reposicao".tags_separacao
          GROUP BY tags_separacao.engenharia, tags_separacao.codreduzido, tags_separacao.descricao, tags_separacao.tamanho, tags_separacao.cor) dt
  GROUP BY dt.engenharia, dt.codreduzido, dt.descricao, dt.tamanho, dt.cor;


-- "Reposicao"."calculoEndereco" source

CREATE OR REPLACE VIEW "Reposicao"."calculoEndereco"
AS SELECT e.codendereco,
    e.codreduzido,
    e.saldo,
    s.endereco,
        CASE
            WHEN s.produto IS NULL THEN e.codreduzido
            ELSE s.produto
        END AS produto,
    s.reservado,
        CASE
            WHEN s.reservado IS NULL THEN e.saldo::double precision
            ELSE e.saldo::double precision - s.reservado
        END AS "SaldoLiquid",
    e.natureza
   FROM "Reposicao".enderecoporsku e
     LEFT JOIN "Reposicao"."saldoLiquidoEndereco" s ON e.codendereco = s.endereco AND e.codreduzido = s.produto;


-- "Reposicao".codreduzido_duplicado source

CREATE OR REPLACE VIEW "Reposicao".codreduzido_duplicado
AS SELECT dt.codreduzido
   FROM ( SELECT "Tabela_Sku".codreduzido,
            count("Tabela_Sku".codreduzido) AS cont
           FROM "Reposicao"."Tabela_Sku"
          GROUP BY "Tabela_Sku".codreduzido) dt
  WHERE dt.cont > 1;


-- "Reposicao"."duplicatasOP" source

CREATE OR REPLACE VIEW "Reposicao"."duplicatasOP"
AS SELECT tt.numeroop,
    tt.contagem
   FROM ( SELECT dt.numeroop,
            count(dt.numeroop) AS contagem
           FROM ( SELECT filareposicaoportag.numeroop,
                    filareposicaoportag.usuario
                   FROM "Reposicao".filareposicaoportag
                  GROUP BY filareposicaoportag.numeroop, filareposicaoportag.usuario) dt
          GROUP BY dt.numeroop) tt
  WHERE tt.contagem > 1;


-- "Reposicao".enderecoporsku source

CREATE OR REPLACE VIEW "Reposicao".enderecoporsku
AS SELECT cc.codendereco,
    r.codreduzido,
    count(r.codbarrastag) AS saldo,
    r.natureza
   FROM "Reposicao".cadendereco cc
     LEFT JOIN "Reposicao".tagsreposicao r ON r."Endereco" = cc.codendereco
  WHERE r.codreduzido IS NOT NULL
  GROUP BY cc.codendereco, r.codreduzido, r.natureza
  ORDER BY cc.codendereco;


-- "Reposicao"."enderecosReposicao" source

CREATE OR REPLACE VIEW "Reposicao"."enderecosReposicao"
AS SELECT cc.codendereco,
    count(r.codbarrastag) AS contagem,
    cc.natureza
   FROM "Reposicao".cadendereco cc
     LEFT JOIN "Reposicao".tagsreposicao r ON r."Endereco" = cc.codendereco
  GROUP BY cc.codendereco, r.natureza
  ORDER BY cc.codendereco;


-- "Reposicao".newview source

CREATE OR REPLACE VIEW "Reposicao".newview
AS SELECT fp.datafinalizacao::date AS datafinalizacao,
    fp."tamCaixa",
    fp.qtdcaixa AS qtdcaixa1,
    fp.tamcaixa2,
    fp.qtdcaixa2,
    fp.tamcaixa3,
    fp.qtdcaixa3,
    fp.tamcaixa4,
    fp.qtdcaixa4
   FROM "Reposicao".finalizacao_pedido fp
  WHERE fp.datafinalizacao IS NOT NULL;


-- "Reposicao".producaorepositoresgeral source

CREATE OR REPLACE VIEW "Reposicao".producaorepositoresgeral
AS SELECT pr.usuario,
    pr.datareposicao,
    pr.datatempo,
    pr.horario
   FROM "Reposicao"."ProducaoRepositores" pr;


-- "Reposicao".producaorepositoresgeral2 source

CREATE OR REPLACE VIEW "Reposicao".producaorepositoresgeral2
AS SELECT pr.usuario,
    pr.datareposicao,
    pr.datatempo,
    pr.horario
   FROM "Reposicao"."ProducaoRepositores2" pr;


-- "Reposicao".producaorepositoresgeral3 source

CREATE OR REPLACE VIEW "Reposicao".producaorepositoresgeral3
AS SELECT pr.usuario,
    pr.datareposicao,
    pr.datatempo,
    pr.horario
   FROM "Reposicao"."ProducaoRepositores3" pr;


-- "Reposicao".relatorio_caixas source

CREATE OR REPLACE VIEW "Reposicao".relatorio_caixas
AS SELECT fp.datafinalizacao::date AS datafinalizacao,
    fp."tamCaixa",
    fp.qtdcaixa AS qtdcaixa1,
    fp.tamcaixa2,
    fp.qtdcaixa2,
    fp.tamcaixa3,
    fp.qtdcaixa3,
    fp.tamcaixa4,
    fp.qtdcaixa4
   FROM "Reposicao".finalizacao_pedido fp
  WHERE fp.datafinalizacao IS NOT NULL;


-- "Reposicao".ritimorepositor source

CREATE OR REPLACE VIEW "Reposicao".ritimorepositor
AS SELECT producaorepositoresgeral.usuario,
    producaorepositoresgeral.datatempo::date AS dia,
    date_trunc('hour'::text, producaorepositoresgeral.datatempo::time without time zone::interval) + floor(date_part('minute'::text, producaorepositoresgeral.datatempo::time without time zone) / 15::double precision) * '00:15:00'::interval AS data_intervalo_min,
    count(*) AS count_tempo
   FROM "Reposicao".producaorepositoresgeral
  WHERE producaorepositoresgeral.usuario IS NOT NULL
  GROUP BY producaorepositoresgeral.usuario, (producaorepositoresgeral.datatempo::date), (date_trunc('hour'::text, producaorepositoresgeral.datatempo::time without time zone::interval) + floor(date_part('minute'::text, producaorepositoresgeral.datatempo::time without time zone) / 15::double precision) * '00:15:00'::interval)
  ORDER BY producaorepositoresgeral.usuario, (producaorepositoresgeral.datatempo::date), (date_trunc('hour'::text, producaorepositoresgeral.datatempo::time without time zone::interval) + floor(date_part('minute'::text, producaorepositoresgeral.datatempo::time without time zone) / 15::double precision) * '00:15:00'::interval);


-- "Reposicao".ritimorepositor2 source

CREATE OR REPLACE VIEW "Reposicao".ritimorepositor2
AS SELECT producaorepositoresgeral2.usuario,
    producaorepositoresgeral2.datatempo::date AS dia,
    date_trunc('hour'::text, producaorepositoresgeral2.datatempo::time without time zone::interval) + floor(date_part('minute'::text, producaorepositoresgeral2.datatempo::time without time zone) / 15::double precision) * '00:15:00'::interval AS data_intervalo_min,
    count(*) AS count_tempo
   FROM "Reposicao".producaorepositoresgeral2
  WHERE producaorepositoresgeral2.usuario IS NOT NULL
  GROUP BY producaorepositoresgeral2.usuario, (producaorepositoresgeral2.datatempo::date), (date_trunc('hour'::text, producaorepositoresgeral2.datatempo::time without time zone::interval) + floor(date_part('minute'::text, producaorepositoresgeral2.datatempo::time without time zone) / 15::double precision) * '00:15:00'::interval)
  ORDER BY producaorepositoresgeral2.usuario, (producaorepositoresgeral2.datatempo::date), (date_trunc('hour'::text, producaorepositoresgeral2.datatempo::time without time zone::interval) + floor(date_part('minute'::text, producaorepositoresgeral2.datatempo::time without time zone) / 15::double precision) * '00:15:00'::interval);


-- "Reposicao".ritimorepositor3 source

CREATE OR REPLACE VIEW "Reposicao".ritimorepositor3
AS SELECT producaorepositoresgeral3.usuario,
    producaorepositoresgeral3.datatempo::date AS dia,
    date_trunc('hour'::text, producaorepositoresgeral3.datatempo::time without time zone::interval) + floor(date_part('minute'::text, producaorepositoresgeral3.datatempo::time without time zone) / 15::double precision) * '00:15:00'::interval AS data_intervalo_min,
    count(*) AS count_tempo
   FROM "Reposicao".producaorepositoresgeral3
  WHERE producaorepositoresgeral3.usuario IS NOT NULL
  GROUP BY producaorepositoresgeral3.usuario, (producaorepositoresgeral3.datatempo::date), (date_trunc('hour'::text, producaorepositoresgeral3.datatempo::time without time zone::interval) + floor(date_part('minute'::text, producaorepositoresgeral3.datatempo::time without time zone) / 15::double precision) * '00:15:00'::interval)
  ORDER BY producaorepositoresgeral3.usuario, (producaorepositoresgeral3.datatempo::date), (date_trunc('hour'::text, producaorepositoresgeral3.datatempo::time without time zone::interval) + floor(date_part('minute'::text, producaorepositoresgeral3.datatempo::time without time zone) / 15::double precision) * '00:15:00'::interval);


-- "Reposicao".ritmosseparador source

CREATE OR REPLACE VIEW "Reposicao".ritmosseparador
AS SELECT tags_separacao.usuario,
    tags_separacao.dataseparacao::date AS dia,
    date_trunc('hour'::text, tags_separacao.dataseparacao::time without time zone::interval) + floor(date_part('minute'::text, tags_separacao.dataseparacao::time without time zone) / 15::double precision) * '00:15:00'::interval AS data_intervalo_min,
    count(*) AS count_tempo
   FROM "Reposicao".tags_separacao
  WHERE tags_separacao.usuario IS NOT NULL
  GROUP BY tags_separacao.usuario, (tags_separacao.dataseparacao::date), (date_trunc('hour'::text, tags_separacao.dataseparacao::time without time zone::interval) + floor(date_part('minute'::text, tags_separacao.dataseparacao::time without time zone) / 15::double precision) * '00:15:00'::interval)
  ORDER BY tags_separacao.usuario, (tags_separacao.dataseparacao::date), (date_trunc('hour'::text, tags_separacao.dataseparacao::time without time zone::interval) + floor(date_part('minute'::text, tags_separacao.dataseparacao::time without time zone) / 15::double precision) * '00:15:00'::interval);


-- "Reposicao"."saldoLiquidoEndereco" source

CREATE OR REPLACE VIEW "Reposicao"."saldoLiquidoEndereco"
AS SELECT pedidossku.endereco,
    pedidossku.produto,
    sum(pedidossku.necessidade) AS reservado
   FROM "Reposicao".pedidossku
  WHERE pedidossku.reservado::text = 'sim'::text AND pedidossku.necessidade > 0::double precision AND pedidossku.endereco <> 'Não Reposto'::text
  GROUP BY pedidossku.endereco, pedidossku.produto;