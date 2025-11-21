# rd_station_challenge

Este repositório trata-se um desafio técnico conforme documentado [aqui](docs/Desafio%20técnico%20-%20Data%20Engineer.pdf).

## Premissas adotadas
As seguintes premissas foram adotadas para a construção da solução:
- Aplicações persistem os arquivos csv em uma camada raw, representada em `data/raw/`;
- O volume de dados é baixo;
- O consumo dos relatórios se dará no schema marts (Gold) do DW;

## Etapas

### Ingestão
O pipeline de ingestão seguirá dessa forma:
1. Leitura do arquivo;
2. Validação com contrato de dados;
3. Full Load na camada raw do DW Postres SQL

### Transformação e Carga
Para limpeza e modelagem de dados foi utilizado o dbt

