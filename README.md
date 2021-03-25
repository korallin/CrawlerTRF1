# Crawler Consulta Processual TRF1

O repositório armazena diferentes _crawlers_ desenvolvidos para a consulta processual por Nome ou CPF no site do Tribunal Regional Federal da 1ª Região (TRF1). Simula uma busca manual no domínio https://processual.trf1.jus.br/consultaProcessual/

#### Formato de Entrada

- Arquivos _.csv_ localizado em **input_data**

#### Formato de Saída

- Arquivos _.json_ localizado em **output_data** com as informações coletadas

#### Crawlers

1. Principal (TRF 1ª Região)
 - _crawler-trf1-cpf.py_
 - _crawler-trf1-nome.py_ 
3. Subseções (Demais unidades)
 - _crawler-sub-cpf.py_
 - _crawler-sub-nome.py_
