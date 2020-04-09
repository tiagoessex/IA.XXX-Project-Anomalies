##########################################
#
#	IDENTIFICAÇÃO DE SITUAÇÕES ANÓMALAS
#
#	requer um ficheiro csv (potencials.csv),
#	onde se encontram identificadas todas
#	as entidade que partilham o mesmo NIFs 
#
#	Campos do potencials.csv:
#		- NIF_NIPC
#		- COUNT(entidade.NIF_NIPC)
#		
#		
#
##########################################

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

import unicodedata
import re
import sys
import mysql.connector
import csv

# if any of these tokens are present in the entity's name
# ignore that entituy and move on to the next
ECO_TOKENS = [
				'bar','restaurante','loja','sa','lda','limitada','sociedade','associacao', 
				'comercio','sport','pastelaria','restaurante','unipessoal','escola',
				'companhia','quinta','hotel','industria','restauracao','hotelaria',
				'uniao','confeccoes','&','electro','artigos','exploracao','distribuicoes',
				'distritbuidora','pastelaria','informatica','sistemas','talho','armazem',
				'pastelaria','jumbo','modelo','minipreco','mercado','bazar','clube','residencial',
				'cafe','guarda','gnr','psp','armadas','esquadra','ldª','combustiveis','galp','palacio',
				'lidl','fnac','apple','worten','gestao','fiscal','saude','comando','parque','inatel',
				'casa','instituicao','pizza','centro','congregacao','igreja','convento','administracao',
				'portugal','secret','ctt','retail','territorial','confeccoeslda','ensitel','padaria',
				'luso','churrasqueira','cooperativa','supermercado','boutique','cantina','forum',
				'vobis','distribuicao','aki','edicoes','continente','agrupamento','modelo','carrefour',
				'women','hiper','nacional','bomba','correios','farmacia','super','animal','plus','transito',
				'controlo','restaurantes','phone','seleccao','ldº','hipermercado','quiosque','importacao',
				'exportacao','ervanaria','telecelular','lavandaria','arepa','5','7','pao','portuguesa','sopas',
				'estaleiro','electrodomesticos','burger','regional','servico','business','moda','intimissimi',
				'eleclerc','tmn','lar','acessorios','center','grupo','cost','inn','ourivesaria','gourmet',
				'sapataria','imp','shop','coffee','club','clube','merlin','exercito','consumo','servicos'
			]


def replacePTChars(s):
	return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))

# note those special characters
def replaceAllNonAlfaNum(s,by_what=" "):
	return re.sub('[^A-Za-z0-9 ªº&]+', by_what, s)
	
# including trailing spaces
def removeAllExtraSpaces(s):
	return " ".join(s.split())



def haveTokens(s):
	tokens = s.split(' ')
	for i in range(len(tokens)):
		if tokens[i] in ECO_TOKENS:
			return True
	return False


def sanitizeNome(nome):
	temp = nome.replace(',', ' ')
	temp = replacePTChars(temp)
	temp = replaceAllNonAlfaNum(temp,'')
	temp = removeAllExtraSpaces(temp)
	return temp



mydb = mysql.connector.connect(
	  host="localhost",
	  user="root",
	  passwd="",
	  database="XXX"
)
mycursor = mydb.cursor()

query_get = '''
		SELECT 
			ID_ENTIDADE,
			NOME,
			MORADA,
			IS_PAI,
			TIPO_ENT_ID_TP_ENT,
			NIF_NIPC			
		from 
			entidade
		where
			NIF_NIPC = %s
		and
			entidade.NOME is not null
		and
			entidade.MORADA is not null
		order by
			ID_ENTIDADE;
'''

query_insert = '''
		insert into 
			entidade_anomalias (id_entidade_1, id_entidade_2)
		VALUES
			(%s,%s);
'''

def getRatioNome(str1, str2):
	return fuzz.ratio(str1,str2)



# minimum similarity ratio [0,100] between 2 strings
# 100 => same
SIM_RATIO_MAX = 40

with open('potencials.csv', mode='r', encoding='utf8') as csv_file:
	csv_reader = csv.DictReader(csv_file,delimiter=',', quotechar = '"')	
	for cnt, line in enumerate(csv_reader):
	
		if cnt % 100 == 0:
			print (cnt)
		if line['NIF_NIPC'] in ['0','000000000','1']:
			continue
		
		mycursor.execute(query_get,(line['NIF_NIPC'],))
		entities = mycursor.fetchall()
		done = []		
		processing = True
		while processing:
			unique = None
			unique_name = None
			for entity in entities:
				if len(done) == len(entities):
					processing = False
					break			
			
				name = sanitizeNome(entity[1].lower())
				
				if not unique and entity[0] not in done and not haveTokens(name):
					unique = entity
					done.append(entity[0])
					unique_name = name
					continue
				
				if entity[0] in done:
					continue	
				if haveTokens(name):
					done.append(entity[0])
					continue	
						
				if getRatioNome(unique_name,name) < SIM_RATIO_MAX and unique[3] == entity[3] and unique[4] == entity[4]:	
					mycursor.execute(query_insert,(entity[0], unique[0]))
					mydb.commit()
					done.append(entity[0])					
					print ("inserted > ", unique[5]) #, done)
					if len(done) == len(entities):
						processing = False
						break


mydb.commit()

