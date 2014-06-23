#!/Library/Frameworks/Python.framework/Versions/3.3/bin/python3
# -*- coding: utf-8 -*-
from urllib.request import urlopen
from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
from pandas import DataFrame

								
def rodaPesquisa():
    client = MongoClient()
    my_db = client["pesquisas_tse"]
    my_collection = my_db["pesquisas"]
    
    pesquisas_antigas = pesquisasAntigas()
        
    lista_ufs = ['BR','AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']
    contador = 1
    resultado = []
    #para cada estado
    for uf in lista_ufs:
        pagina = 1
        #limite máximo de páginas, escolhido arbitrariamente
        while pagina < 10:
            url = "http://pesqele.tse.jus.br/pesqele/publico/pesquisa/Pesquisa/consultarPesquisasPublica.action?dataFimRegistro=&action:pesquisa/Pesquisa/consultarPesquisasPublica=Consultar&municipioSelecionado=&d-4021255-p="+str(pagina)+"&dataInicioRegistro=&ufSelecionada="+uf+"&pesquisa.numeroProtocolo=&eleicaoSelecionada=3&empresaSelecionadaPublica="
            page = BeautifulSoup(urlopen(url).read())
            links = page.findAll('a',{'class':'visualizar'})

            #se não tiver link nenhum na página, saia do loop
            if (not links): break
            
            #se tiver, escreve no arquivo
            for link in links:
                #se o id do link já não estiver no antigo antigo
                id = link['href'].split("=")[2]
                if id not in pesquisas_antigas:
                    print(str(contador) + " - " +uf+" - "+link['href'])
                    #scrapeia pagina
                    resultado.append(adicionaPagina(link['href']))
                    contador +=1
            pagina += 1
    print(str(contador-1) +" novas pesquisas foram adicionadas ao banco de dados")
    exportar = DataFrame(resultado)
    del exportar["_id"]
    exportar.to_csv("/Users/rodrigoburgarelli/github/Pesquisas TSE/resultado_pesquisas.csv",index=False)
        
def scrape_pagina(url):
    page = BeautifulSoup(urlopen(url).read())
    
    #pega todo o campo que tem todas as infos, retira apenas as strings e quebra por linha
    campos_bruto = str(page.findAll('fieldset')[0].getText())
    campos_consertando = campos_bruto.split('\n')
    
    #retira os characteres /r e /t e os elementos vazios
    campos_final = [c.replace("\t","").replace("\r","") for c in campos_consertando if (c != "") & (c.replace("\t","").replace("\r","") != "")]
    categorias = ['Número do protocolo:',
                  'Data de registro:',
                  'Data de divulgação:',
                  'Empresa contratada:',
                  'Eleição:',
                  'Cargo(s):',
                  'Abrangência:',
                  'Contratante:',
                  'Origem dos recursos:',
                  'Pagante do trabalho:',
                  'Valor (R$):',
                  'Estatístico responsável:',
                  'Registro do estatístico no CONRE:',	
                  'Registro da empresa no CONRE:',
                  'Data de início:',
                  'Data de término:',
                  'Entrevistados:',
                  'Metodologia de pesquisa:',	
                  'Plano amostral e ponderação quanto a sexo, idade, grau de instrução e nível econômico do entrevistado, margem de erro e nível de confiança:',
                  'Sistema interno de controle e verificação, conferência e fiscalização da coleta de dados e do trabalho de campo:',
                  'o pedido de registro será complementado pela entrega destes dados ao Tribunal Eleitoral em um prazo de até 24 horas, contado da divulgação do respectivo resultado):']
    
    
    traducao = ['protocolo',
                  'data_registro',
                  'data_divulgacao',
                  'empresa',
                  'eleicao',
                  'cargo',
                  'abrangencia',
                  'contratante',
                  'origem_recursos',
                  'pagante',
                  'valor',
                  'estatistico',
                  'registro_estatistico',	
                  'registro_empresa:',
                  'data_inicio',
                  'data_termino',
                  'entrevistados',
                  'metodologia',	
                  'amostra',
                  'controle',
                  'locais']
    
    #acha o índice das categorias na lista dos campos
    indices = [i for i,x in enumerate(campos_final) if (x in categorias) & (x != "")]

    #guarda tudo em um dicionário
    pesquisa = {}
    trad = 0
    categorias.append("Questionário completo aplicado ou a ser aplicado (formato PDF):")
    
    for i in indices:
        valor = campos_final[i+1].strip()
        if (valor in categorias): valor = "NA" #checa se existe ou não o dado procurado
        pesquisa[traducao[trad]] = valor
        trad +=1
    
    pesquisa["url"] = url
    pesquisa["uf"] = pesquisa["protocolo"][0:2]
    
    #arruma as datas para ficar YY-mm-dd
    data_termino = pesquisa["data_termino"].split("/")
    pesquisa["data_termino"] = "20"+data_termino[2]+"-"+data_termino[1]+"-"+data_termino[0]
    data_registro = pesquisa["data_registro"].split("/")
    pesquisa["data_registro"] = data_registro[2]+"-"+data_registro[1]+"-"+data_registro[0]
    data_inicio = pesquisa["data_inicio"].split("/")
    pesquisa["data_inicio"] = "20"+data_inicio[2]+"-"+data_inicio[1]+"-"+data_inicio[0]
    data_divulgacao = pesquisa["data_divulgacao"].split("/")
    pesquisa["data_divulgacao"] = data_divulgacao[2]+"-"+data_divulgacao[1]+"-"+data_divulgacao[0]
    
    return pesquisa

def pesquisasAntigas():
    client = MongoClient()
    my_db = client["pesquisas_tse"]
    my_collection = my_db["pesquisas"]
    resultado = my_collection.find()
    lista_links = [a["url"] for a in resultado]
    lista_ids = [l.split("=")[2] for l in lista_links]
    return lista_ids
    
def adicionaPagina(link):
    link = "http://pesqele.tse.jus.br/"+link
    client = MongoClient()
    my_db = client["pesquisas_tse"]
    my_collection = my_db["pesquisas"]
    resultado = scrape_pagina(link)
    my_collection.insert(resultado)   
    return resultado
    
def consultaPesquisas():
    client = MongoClient()
    my_db = client["pesquisas_tse"]
    my_collection = my_db["pesquisas"]
    resultado = []
    for a in my_collection.find():        
        resultado.append(a)
    
    print(resultado[1])
    exportar = DataFrame(resultado)
    del exportar["_id"]
    exportar.to_csv("/Users/rodrigoburgarelli/github/Pesquisas TSE/resultado_pesquisas.csv",index=False)

#print(pesquisasAntigas())

rodaPesquisa()
#consultaPesquisas()

