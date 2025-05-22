# IFOO-CASE - DATA-ENGINEER

    - Faça as instalações necessárias de acordo com o requirements.txt

    - o código que irá criar a camada raw está no arquivo do tipo .py "src/raw"
        > Rode esse script com o comando "python src/raw.py", responsável por fazer:
            * A leitura dos dados no site da agência responsável por licenciar e regular os táxis na cidade de NY: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
            * Ingestão dos dados na camada raw do S3

    - Valide os dados da camada raw no arquivo do tipo .ipynb "data_validation_raw"

    - o código que irá criar a camada silver está no arquivo do tipo .py "src/silver"
        > Rode esse script com o comando "spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.375 src/silver.py", para garantir a configuração dos pacotes do Hadoop e AWS SDK. O script é responsável por fazer:
            * A leitura dos dados na camada raw
            * Executa toda a limpeza, transformação e validação dos dados
            * Ingestão dos dados na camada silver do S3

    - Valide os dados da camada silver no arquivo do tipo .ipynb "data_validation_silver"

