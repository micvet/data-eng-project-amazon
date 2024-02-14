# Projeto - Ingestão e transformação de dados com Azure

O objetivo deste projeto foi aplicar os conhecimentos nas ferramentas da plataforma Azure, suas aplicações e integrações.

O projeto foi desenvolvido utilizando um dataset da plataforma [Kaggle](https://www.kaggle.com/datasets/mbogernetto/brazilian-amazon-rainforest-degradation/data), que contém dados sobre desmatamentos, queimadas e fenômenos naturais que afetaram o Brasil ao longo do tempo.

Foi realizado o upload dos arquivos CSV contidos nesse dataset para a pasta ["data"](https://github.com/micvet/data-eng-project-amazon/tree/main/data) deste repositório do GitHub.

A partir daí, utilizei o Azure Data Factory para criar um novo pipeline que realizasse a integração e obtenção dos arquivos. Esses arquivos foram alocados em diretórios previamente criados no Azure Data Lake Gen2. Após isso, foram realizadas algumas transformações nos dados por meio do Azure Databricks, utilizando Pyspark. Os dados com as devidas transformações foram então devolvidos ao Data Lake e, posteriormente, foi criado um novo workspace no Azure Synapse, onde os dados foram utilizados para criar um banco de dados.




![image](https://github.com/micvet/data-eng-project-amazon/assets/86981990/f82a8814-02c4-4f0d-b7c3-438b29a2ecae)

