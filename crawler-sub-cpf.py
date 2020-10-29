import json
import scrapy
import requests
import pandas as pd
import unidecode
from scrapy.crawler import CrawlerProcess
from scrapy.http import FormRequest
from bs4 import BeautifulSoup

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
			url_full = 'https://processual.trf1.jus.br' + url.replace('&mostrarBaixados=S', '&mostrarBaixados=N')
			yield response.follow(url=url_full, callback=self.parse_second, cb_kwargs=dict(metadata=user_data))

	def parse_second(self, response, metadata):

		user_data = metadata.copy()

		p_links = response.css('tbody a::attr(href)')
		p_links_to_follow = p_links.extract()

		# Encaminha os links para o próximo parser
		for url in p_links_to_follow:
			yield response.follow(url=url, callback=self.parser_final, cb_kwargs=dict(metadata=user_data))

	def parser_final(self, response, metadata):

		url_base = 'https://processual.trf1.jus.br'

		# Aba Processo
		process_table = response.css('div#aba-processo')

		data_list = list()

		for i in range(1, len(process_table.css('table > tbody tr')) + 1):

			data_line = process_table.css('table > tbody tr:nth-of-type(%s)' % i)

			# Cabeçalho
			data_th = data_line.css('th::text')
			header = unidecode.unidecode(data_th.get().strip().lower()).replace(':', '').replace(' ', '_')

			# Dados
			data_td = data_line.css('td::text')

			# Linhas com mais de um campo de resposta
			if len(data_td) > 1:

				# Coleta o conteúdo textual
				data = [s.get().strip() for s in data_td]

				# Remover Strings vazias
				data = [i for i in data if i]

				data_list.append([header, data])
				continue

			# Verificar links no campo de resposta
			data_td_link_text = data_line.css('td a::text')
			data_td_link_href = data_line.css('td a::attr(href)')

			if len(data_td_link_href) > 0:
				data = dict()

				data['processo'] = data_td_link_text.get().strip()
				data['url'] = url_base + data_td_link_href.get().strip()

				data_list.append([header, data])
				continue

			data = data_td.get()
			data_list.append([header, data])

		# JSON final
		results_dict = dict()

		results_dict['nome_parte'] = metadata['nome_parte']
		results_dict['cpf_cnpj'] = metadata['cpf_cnpj']

		for line in data_list:

			if line[1]:
				results_dict[line[0]] = line[1]
			else:
				results_dict[line[0]] = 'Não informado'

		results_dict['secao'] = metadata['secao']
		results_dict['search_url'] = response.url

		results_list.append(results_dict)


if __name__ == '__main__':

	# List to save the output_data collected
	results_list = list()

	# url = 'https://processual.trf1.jus.br/consultaProcessual/index.php?secao=TRF1'
	# req_s = requests.get(url)
	# soup = BeautifulSoup(req_s.content, 'html.parser')
	#
	# # Lista com as opções de seção/subseção
	# opts = [s.get('value') for s in soup.select('select.consulta option') if len(s.get('value')) > 0]
	# opts = list(dict.fromkeys(opts))
	# opts = opts[1:]

	# Caso queira explorar todas as subseções
	# Descomentar as linhas 147-154
	# E comentar a linha abaixo
	opts = ['DF', 'GO', 'MG']

	# Initiate a CrawlerProcess
	process = CrawlerProcess()

	# Tell the process which spider to use
	process.crawl(Trf1Spider)

	# Start the crawling process
	process.start()

	# Save the list of dicts
	with open('output_data/results-sub-cpf.json', 'w', encoding='utf8') as f:
		json.dump(results_list, f, ensure_ascii=False)