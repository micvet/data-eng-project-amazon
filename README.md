# data-eng-project-amazon


About Dataset
Context
The Amazon rainforest is a moist broadleaf tropical rainforest in the Amazon biome that covers territory belonging to nine nations. The majority of the forest is contained within Brazil, with 60% of the rainforest, followed by Peru with 13%, Colombia with 10%, and with minor amounts in Venezuela, Ecuador, Bolivia, Guyana, Suriname and French Guiana.

The region provides important benefits to communities living near and far. Nearly 500 indigenous communities call the Amazon rainforest home. It’s a highly biodiverse ecosystem, home to untold species of plants and animals. The rainforest can create its own weather and influence climates around the world. Unfortunately, the fragile ecosystem faces the constant threat of deforestation and fires (for natural or anthropogenic causes).

Deforestation happens for many reasons, such as illegal agriculture, natural disasters, urbanization and mining. There are several ways to remove forests - burning and logging are two methods. Although deforestation is happening all over the world today, it is an especially critical issue in the Amazon rainforests, as the only large forest still standing in the world. There, the species of plants and animals they harbor have been disappearing at an alarming rate.

Content
This dataset refers to 3 files:

'inpe_brazilian_amazon_fires_1999_2019' : amount of fire outbreaks in Brazilian Amazon by state, month and year, from 1999 to 2019. The original data are public and were extracted from INPE website on December 13th 2019, always from the filtering for the reference salellite and aggregated using Postgres SQL so that you could work with lighter files.
Program: BDQ (Banco de Dados de Queimadas, or Fires Database).
Methodology: detects fire outbreaks through satellite images, updated every 3 hours.

'def_area_2004_2019' : deforestation area (km²) by year and state, from 2004 to 2019. The data are public and were extracted from INPE website on December 16th 2019. It was already aggregated, so, no data process was made.
Program: PRODES (Programa de Monitoramento da Floresta Amazônica Brasileira por Satélite, or Brazilian Amazon Rainforest Monitoring Program by Satellite).
Methodology: maps primary forest loss using satellite imagery, with 20 to 30 meters of spatial resolution and 16-day revisit rate, in a combination that seeks to minimize the problem of cloud cover and ensure interoperability criteria.

'el_nino_la_nina_1999_2019' : Data about start year, end year, and severity of 2 of the most important climatic phenomena.
Data were extracted from Golden Gate Weather Services on December 20th 2019 and were unpivoted.
