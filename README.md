🔧 Instruções de Execução

Este repositório é privado: https://github.com/talita-costa/ifood-case

1. Pré-requisitos

Instale as dependências requeridas com pip install

2. Carregamento e Tratamento dos Dados

Devido ao tamanho das bases de dados, utilize o script data_loading.py para realizar o download e o pré-processamento:

python data_loading.py

3. Execução das Análises

Execute os notebooks na ordem abaixo, conforme o objetivo de cada um:

campaign_performance.ipynb: análise do sucesso da campanha.

financial_viability.ipynb: análise da viabilidade financeira da campanha.

campaign_performance_segmented.ipynb: análise segmentada dos clientes do grupo target, considerando o ticket médio por faixa de valor mínimo dos restaurantes.

Observação: Esta análise foi usada como base para propor uma nova estratégia de teste A/B.

4. Apresentação

Consulte o arquivo PDF de apresentação para visualizar as conclusões e sugestões propostas aos líderes de negócio.