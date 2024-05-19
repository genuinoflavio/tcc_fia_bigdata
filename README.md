Repositorio destinando ao desenvolvimento do TCC.

Instruções para rodar o projeto:
PARA EXECUTAR ESSE PROJETO

passo 1 - buildar a imagem do airflow
        - cd Airflow (tem ir para o diretorio AIRFLOW)
        - docker build -f .\Dockerfile -t tcc-airflow .
        -cd .. (para voltar o diretorio)

passo 2 - buildar a imagem do jupyter notebook
        -cd jupyternotebook (tem ir para o diretorio AIRFLOW)
        -docker build -f .\DockerFile -t tcc-jupyternotebook .
        -cd ..

passo 3 - Executar o docker compose para subir todos os serviços
        - docker-compose -f .\docker-compose.yml up -d
        - se quiser derrubar todos os serviços de uma vez executar: docker-compose -f .\docker-compose.yml down 



USUARIO e SENHA do AIRFLOW e MINIO
-user: tcc_fia
-senha: tcc_fia