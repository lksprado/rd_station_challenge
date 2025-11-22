# rd_station_challenge

Este repositório trata-se um desafio técnico conforme documentado [aqui](docs/Desafio%20técnico%20-%20Data%20Engineer.pdf).

## Premissas adotadas
As seguintes premissas foram adotadas para a construção da solução:
- Aplicações persistem os arquivos csv em uma camada raw, representada em `data/raw/`;
- O volume de dados é baixo;
- O consumo dos relatórios se daria no schema marts (Gold) do DW;

## Etapas

### Ingestão
O pipeline de ingestão seguirá dessa forma:
1. Leitura do arquivo;
2. Full Load na camada raw do DW Postres SQL

### Transformação e Carga
Para limpeza e modelagem de dados foi utilizado o dbt

### Observações
A segmentação por país na tabela fato não condiz com a tabela de metas porque essa última não possui essa coluna;\
O horizonte de datas foi criado a partir de `created_date`;\
`last_touch_date` foi considerado como último_horário_conversao_dia;\
Contrato de Dados aplicado na camada Staging(Clean) dos modelos através de testes\
Documentação das tabelas em `models/schema.yml`

## Como Rodar
1. Clonar o repositório.
2. Criar e ativar um ambiente virtual (ex.: `python -m venv .venv && source .venv/bin/activate`) e instalar dependências: `pip install -e .`.
3. Configurar as variáveis de conexão com Postgres copiando `.env-example` para `.env` e preenchendo `DB_USER`, `DB_PW`, `DB_HOST`, `DB_PORT` e `DB_NAME` (schema alvo).
4. Garantir que o Postgres tem um schema `raw` acessível ao usuário informado.
5. Rodar a ingestão para popular as tabelas raw: `python src/ingestion.py`.
6. Configurar o profile do dbt:
   - Opção A (preferida): apontar o dbt para o `profiles.yml` que está no diretório pai do projeto exportando `DBT_PROFILES_DIR=$(pwd)/..`.
   - Opção B: copiar/atualizar o conteúdo de `profiles.yml` para `~/.dbt/profiles.yml`.
7. Executar as transformações dbt dentro de `rd_station_dw`: `cd rd_station_dw && dbt build`.
