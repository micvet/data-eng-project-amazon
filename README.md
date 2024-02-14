# Projeto - Ingestão e transformação de dados com Azure

O objetivo deste projeto foi aplicar os conhecimentos nas ferramentas da plataforma Azure, suas aplicações e integrações.

O projeto foi desenvolvido utilizando um dataset da plataforma [Kaggle](https://www.kaggle.com/datasets/mbogernetto/brazilian-amazon-rainforest-degradation/data), que contém dados sobre desmatamentos, queimadas e fenômenos naturais que afetaram o Brasil ao longo do tempo.

Foi realizado o upload dos arquivos CSV contidos nesse dataset para a pasta ["data"](https://github.com/micvet/data-eng-project-amazon/tree/main/data) deste repositório do GitHub.

A partir daí, utilizei o Azure Data Factory para criar um novo pipeline que realizasse a integração e obtenção dos arquivos. Esses arquivos foram alocados em diretórios previamente criados no Azure Data Lake Gen2. Após isso, foram realizadas algumas transformações nos dados por meio do Azure Databricks, utilizando Pyspark. Os dados com as devidas transformações foram então devolvidos ao Data Lake e, posteriormente, foi criado um novo workspace no Azure Synapse, onde os dados foram utilizados para criar um banco de dados.

![image](https://github.com/micvet/data-eng-project-amazon/assets/86981990/f82a8814-02c4-4f0d-b7c3-438b29a2ecae)

## Passo-a-passo:

1) Crie um novo Storage Account. As configurações utilizadas foram as padrão, com  a seleção da opção Habilitar Name pace Hierárquico, para melhor organização dos arquivos.

<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/61704ab2-fe0b-4b9f-8ff4-4d69ff403d3f' height='100'/>
<div/><br> 

Habilitando namespace hierárquico:<br>

<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/61daf9ef-877f-46e0-be62-7d2fb8e44e9a' height='100'/>
<div/><br> 

- Dentro do container, crie duas pastas. No meu caso, criei duas pastas: raw-data receberá os dados crus, e transformed-data, que receberá os dados após a transformação.<br>


2) Agora, crie um novo Registro de App, buscando pelo recurso Registros de Aplicativo. Isso gerará credenciais, que usaremos posteriormente em processos de autenticação. Salve o Application Client ID e o Directory ID que serão exibidos.<br>
   
<br><div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/7e64f864-b3ef-4812-8381-df1ebcb03b7b' height='350'/>
<div/><br> 


- Posteriormente, ainda no serviço de Registros de Apps, na aba à esquerda, selecione a opção Certificados e Segredos, crie um novo segredo do cliente. Copie o valor da Secret Key que será gerado.
   
<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/c91d1cbf-d36a-4f6c-a53c-3826eb87b35c' height='250'/>
<div/><br> 


3) Volte no seu container do Storage Account, e nas propriedades, selecione (IAM) Controle de Acesso. Clique em "Adicionar Atribuição de Função". Será aberta uma lista com diversas configurações. Selecione "Colaborador de Dados do Storage Blob" ou "Storage Blob Data Contributor"(em inglês).Essa opção cncede permissões  para acessar e gerenciar dados dentro do contêiner de Blob Storage, que permitirá, dentre outras coisas, a leitura e gravação de dados.

  * Selecione a opçao de avançar e depois "Atribuir Acesso a". No campo de busca, digite o nome que foi dado ao seu App anteriormente, realize a seleção e selecione "Examinar + Atribuir".
  * O container está pronto para receber os dados.

<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/8fb69d57-647a-4999-8c1a-d20b0b86a342' height='350'/>
<div/><br> 

4) Agora vamos criar um data factory. Busque pelo serviço no Azure e selecione a opção de criar um novo Data Factory. As configurações padrão podem ser utilizadas.
   
<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/07744b6a-f45a-46aa-8da5-7c9bd565bc75' height='200'/>
<div/><br>  
  
- Acesse a Data Factory criada e na aba de ferramentas, selecione a opção Autor > Pipelines. Selecione então a opção Mover e Transfomar > Copiar Dados. Arraste o ícone para a área de trabalho.

<br><div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/986d51da-487d-4c10-b7fa-e9e3fb2ac643' height='350'/>
<div/><br>

- Nas propriedades do Copiar Dados, selecione a opção "Origem". Será aberto um menu, com diversas opções de serviços para ingestão de dados. Busque por HTTP, pois iremos utilizar a [RAW URL](https://github.com/micvet/data-eng-project-amazon/blob/main/scripts/raw-urls.txt) dos arquivos no GITHUB. Selecione o tipo de arquivo (estamos usando CSV) e selecione a opção de criar um serviço vinculado.
   
<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/3d148984-cd95-4929-b526-41d5f5392fe7' height='300'/>
<div/><br> 

- Configure o Serviço Vinculado: altere o nome, insira a URL e selecione o tipo de autenticação como "Anônimo".  
<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/4d811401-6706-4c5e-a7d1-8d5e9addcd49' height='300'/>
<div/><br>  
  
  * Após realizar todas as configurações, selecione "ok"<br>
<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/ff6c27cf-e932-4aaa-b022-96f9f1749aa1' height='350'/>
<div/><br> 

- Agora você deve configurar o coletor, que definirá o destino dos dados. Selecione o conjunto de dados com que está trabalhando clique na opção "Novo".
    
<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/33aea9a7-8a23-43a4-b635-fee2262e189a' height='300'/>
<div/><br> 

- Será aberto um outro menu para escolha dos serviços. Selecione o Azure Data Lake Gen2, e criaremos um novo serviço utilizando o Data Lake no Azure Storage criado previamente.

<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/b073267b-86f1-42c6-953f-6f4c9ccc9fe0' height='400'/>
<div/><br> 

- Preencha o diretório e no último campo, o nome que será dado ao arquivo extraído.

<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/1490b449-b2ad-42f6-8aa0-65fa722efeb8' height='300'/>
<div/><br> 

- Valide as configurações realizadas e repita o procedimento para os demais arquivos. Após o pipeline rodar, os arquivos devem ter sido direcionados ao container com sucesso, na pasta selecionada.

<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/e144b506-6185-4c04-b7b4-fad6f704317c' height='350'/>
<div/><br>

5) Na sequencia, busque pelo serviço Azure Data Bricks e crie um novo Workspace. As configurações podem ser padrão.

- Inicie o Work Space. Uma vez dentro do workSpace do Data Bricks, crie um novo node (Compute), necessário para rodar os scripts.

<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/188add53-cb4c-41d7-87b1-9753c89dd1d3' height='350'/>
<div/><br> 

- Após o node estar up, crie um novo notebook clicando em novo. <br>

- Com o notebook aberto, inicie as configurações que permitirão acesso aos arquivo no Data Lake.<br>

- Se certifique de preencher:<br>
  -- Client id
  -- Key id
  -- Directory
  -- Nome do seu container e storage account<br>

- Abaixo está o script utilizado. Copie e cole no seu notebook, substituindo os dados pelos necessários.<br>

``` configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "SEU CLIENT ID",
"fs.azure.account.oauth2.client.secret": 'SUA KEY ID', 
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/SEU DIRECTORY ID/oauth2/token"}
```

- Monte um diretório para que o Databricks possa realizar o acesso e manipulação dos arquivos. Altere os dados conforme o necessário:<br> 

```dbutils.fs.mount( source = "abfss://NOME--DO-SEU-CONTAINER@NOME-STORAGE-ACCOUNT.dfs.core.windows.net", #PREENCHA O NOME DO SEU CONTAINER E STORAGE ACCOUNT
mount_point = "/mnt/NOMEIE-SEU-DIR", #NOMEIE O SEU DIRETÓRIO
extra_configs = configs)<br>
```

- Após rodar esse script, valide o acesso:<br> 

```dbutils.fs.mount( source = "abfss://amazonia-data@micursode.dfs.core.windows.net", # contrainer@storageacc 
mount_point = "/mnt/NOME-DO-SEU-DIR", 
extra_configs = configs)<br>
```

- Após isso, você poderá executar as transformações e operações necessárias nos dados. As transformações realizadas neste projeto se encontram no script abaixo, utilizando PySpark:<br> 

  - [transform_data_databricks.ipynb](https://github.com/micvet/data-eng-project-amazon/blob/main/scripts/transform_data_databricks.ipynb)<br> 

- Por fim, os dados moficados foram salvos usando o script abaixo:<br>

```#Salvando os dados após as transformações
#Repartition = indica em quantas partes o arquivo deve estar dividido, uma vez que arquivos de operações que envolvem bigdata podem necessitar de várias repatições para melhor processamento.
#A linha é composta por nome-do-df.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/NOME-DIR-MONTADO/NOME-PASTA-DESTINO-DATA-LAKE/NOME-ARQUIVO-FINAL")<br> 

elnino.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/amazondb/transformed-data/elnino")
amazon_deforestation.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/amazondb/transformed-data/amazon_deforestation")
amazon_fires.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/amazondb/transformed-data/amazon_fires")<br> 
```

- Os dados foram salvos no diretório "transformed_data", do container.<br> 

<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/567430de-141d-4946-85b5-d8a7ebb574c1' height='300'/>
<div/><br>

6) Agora, crie um novo workspace no Azure Synapse. Mantenha as configurações padrão e escolha o armazenamento na Storage Account criada anteriormente.<br> 

<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/3cdda843-fd95-48c6-8009-6f9f88fe8354' height='300'/>
<div/><br> 

- Com o synapses ativo, crie um novo banco de dados selecionando "Banco de Dados Lake" e selecione a opção de criar uma nova tabela de data lake. <br> 

- Preencha os campos solicitados com o nome da tabela, serviço vinculado default e o diretório para o arquivo com os dados que popularão a tabela. Neste caso, será usado o arquivo da pasta transformed-data, destino dos arquivos que foram transformados usando o Azure Data Bricks.<br> 

<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/add49cd3-d585-481e-996c-6858e5dfe37a' height='350'/>
<div/><br> 

- Clique em continuar e nas propriedades adicionais, certifique-se de selecionar a opção "Inferir nomes de coluna", para a primeira linha. <br> 

<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/3764d46b-9286-4f1f-b9e5-81559cc1fc9e' height='350'/>
<div/><br> 

- Ao final, publique as alterações.
- Clicando com o botao esquerdo do mouse no símbolo da database, selecione a opção de criar uma nova consulta SQL.<br> 
<div align='left'>
   <img src='https://github.com/micvet/data-eng-project-amazon/assets/86981990/c63f9e2d-4229-4d40-bf81-8c4a7b10c0c1' height='350'/>
<div/><br> 

  - Algumas das consultas que podemos realizar se encontram em [consultadados.sql](https://github.com/micvet/data-eng-project-amazon/blob/main/scripts/consulta_dados.sql)

