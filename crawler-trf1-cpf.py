import json
import scrapy
import time
import pandas as pd
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest

import logging
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

class Trf1Spider(scrapy.Spider):

	name = 'trf1_spider'

	def start_requests(self):

		data = list()

		df = pd.read_csv('./input_data/teste-cpf.csv', encoding='utf8', sep=',')

		# URL base para realizar o POST request
		url_base = 'https://processual.trf1.jus.br/consultaProcessual/parte/listarPorCpfCnpj.php'

		for index, line in df.iterrows():

			cpf_parte = str(line[0]).replace('.', '').replace('-', '')

			for opt in opts:

				# Armazenar informações da parte
				user_data = dict()
				user_data['cpf_cnpj'] = cpf_parte
				user_data['mostrarBaixados'] = 'S'
				user_data['secao'] = opt
				user_data['enviar'] = 'Pesquisar'
				user_data['nmToken'] = 'cpfCnpjParte'

				data.append(user_data)

		for user_data in data:
			yield FormRequest(url=url_base, callback=self.parse_first, formdata=user_data,
							  cb_kwargs=dict(metadata=user_data), dont_filter=True)

	def parse_first(self, response, metadata):

		user_data = metadata.copy()

		p_links = response.css('tbody a::attr(href)')
		p_name = response.css('tbody a::text').get()

		if p_name is not None:
			user_data['nome_parte'] = p_name

		# Extrair os links (lista de strings)
		p_links_to_follow = p_links.extract()

		# Encaminha os links para o próximo parser
		for url in p_links_to_follow:
			url_full = 'https://processual.trf1.jus.br' + url.replace('&mostrarBaixados=S', '&mostrarBaixados=S')
			yield response.follow(url=url_full, callback=self.parse_second, cb_kwargs=dict(metadata=user_data))

	def parse_second(self, response, metadata):

		user_data = metadata.copy()

		p_links = response.css('tbody a::attr(href)')

		p_links_to_follow = p_links.extract()

		# Encaminha os links para o próximo parser
		for url in p_links_to_follow:
			yield response.follow(url=url, callback=self.parser_final, cb_kwargs=dict(metadata=user_data))

	def parser_final(self, response, metadata):

		# Aba Processo
		process_table = response.css('div#aba-processo')

		data_list = list()

		# Processo, Nova Numeração, Grupo, Assunto
		# Data de Autuação, Órgão Julgador, Juíz Relator
		for i in range(1, 8):

			data_line = process_table.css('table > tbody tr:nth-of-type(%s)' % i)
			data_th = data_line.css('th::text').extract_first().strip()
			data_td = str(data_line.css('td::text').extract_first()).strip()

			data_list.append([data_th, data_td])

		# Processo Originário
		orgao_list = list()
		url_base = 'https://processual.trf1.jus.br'

		orgao_julgador_header = process_table.css('table > tbody tr:nth-of-type(8)')
		orgao_julgador_data_th = orgao_julgador_header.css('th::text').extract_first().strip()
		orgao_julgador_data_text_td = orgao_julgador_header.css('td a::text').extract_first().strip()
		orgao_julgador_data_link_td = orgao_julgador_header.css('td a::attr(href)').extract_first().strip()
		orgao_list.append(orgao_julgador_data_text_td)
		orgao_list.append(url_base + orgao_julgador_data_link_td)
		data_list.append([orgao_julgador_data_th, orgao_list])

		# Armazena os valores das colunas
		column_data = [td for (th, td) in data_list]

		# JSON final
		results_dict = dict()

		results_dict['nome_parte'] = metadata['nome_parte']
		results_dict['cpf_cnpj'] = metadata['cpf_cnpj']
		results_dict['processo'] = column_data[0]
		results_dict['nova_numeracao'] = column_data[1]
		results_dict['grupo'] = column_data[2]
		results_dict['assunto'] = column_data[3]
		results_dict['data_autuacao'] = column_data[4]
		results_dict['orgao_julgador'] = column_data[5]
		results_dict['juiz_relator'] = column_data[6]
		results_dict['processo_originario'] = column_data[7]
		results_dict['secao'] = metadata['secao']
		results_dict['search_url'] = response.url

		results_list.append(results_dict)


if __name__ == '__main__':

	# Start time
	st = time.time()

	results_list = list()

	opts = ['TRF1']

	# Initiate a CrawlerProcess
	process = CrawlerProcess()

	# Tell the process which spider to use
	process.crawl(Trf1Spider)

	# Start the crawling process
	process.start()

	# Save the list of dicts
	with open('output_data/results-trf1-cpf.json', 'w', encoding='utf8') as f:
		json.dump(results_list, f, ensure_ascii=False)

	# Finish time
	ft = time.time()

	print('Tempo Total de Execução: {:.2f} segundos'.format(ft - st))