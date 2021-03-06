# Vacinacao_Covid19
Dados da Campanha Nacional de Vacinação contra Covid-19

# Sobre o projeto

Trata-se de um projeto sugerido pelo pessoal da Semantix Academy na pessoa do Rodrigo Rebouças para treinamento e validação dos conhecimentos repassados no curso de Engenharia de Dados que iniciou em 31/01/2022.

## Realizar as tarefas enumeradas utilizando dados da Saúde:

Dados: https://mobileapps.saude.gov.br/esus-vepi/files/unAFkcaNDeXajurGB7LChj8SgQYS2ptm/04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021.rar

Referência das Visualizações:

• Site: https://covid.saude.gov.br/

• Guia do Site: Painel Geral

1. Enviar os dados para o hdfs 
  
    docker cp arquivos/ namenode:/
    
    docker exec -it namenode ls /arquivos
    
    hdfs dfs -mkdir -p /user/marcos/data/covid
    
    hdfs dfs -ls /user/marcos/data/covid
    
    hdfs dfs -put /arquivos/*.* /user/marcos/data/covid/
    
  
2. Otimizar todos os dados do hdfs para uma tabela Hive particionada por
município.

    Arquivo: hive.sql (Foi utilizado dbeaver para execução dos comandos)
  
3. Criar as 3 vizualizações pelo Spark com os dados enviados para o HDFS: (Arquivo: Desafio1.ipynb)
<image src=https://raw.githubusercontent.com/marcosgoval/Vacinacao_Covid19/main/assets/figura1.png>

4. Salvar a primeira visualização como tabela Hive (Arquivo: Desafio1.ipynb)
5. Salvar a segunda visualização com formato parquet e compressão snappy (Arquivo: Desafio1.ipynb)
6. Salvar a terceira visualização em um tópico no Kafka (Arquivo: Desafio1.ipynb)
7. Criar a visualização pelo Spark com os dados enviados para o HDFS: (Arquivo: Desafio1.ipynb)
<image src=https://raw.githubusercontent.com/marcosgoval/Vacinacao_Covid19/main/assets/figura2.png>
8. Salvar a visualização do exercício 6 em um tópico no Elastic

9. Criar um dashboard no Elastic para visualização dos novos dados enviados

# Autor

Marcos Garcia Meirelles

https://www.linkedin.com/in/marcosgarciati/
