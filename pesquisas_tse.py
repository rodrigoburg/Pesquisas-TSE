#!/Library/Frameworks/Python.framework/Versions/3.3/bin/python3
# -*- coding: utf-8 -*-
from urllib.request import urlopen
from bs4 import BeautifulSoup
import csv


								
def pega_links():
    pesquisas_antigas = pesquisasAntigas()
    with open("links_pesquisas.csv", "a", encoding='UTF8') as saida:
        escrevedor = csv.writer(
            saida,
            delimiter=',',
            quotechar='"',
            quoting=csv.QUOTE_ALL)
        
        lista_ufs = ['BR','AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']
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
                        print(uf+" - "+link['href'])
                        escrevedor.writerow([uf,link['href']])
                    
                pagina += 1
        
        
def scrape_pagina(url):
    page = BeautifulSoup(urlopen(url).read())
    
    #pega todo o campo que tem todas as infos, retira apenas as strings e quebra por linha
    campos_bruto = str(page.findAll('fieldset')[0].getText())
    campos_consertando = campos_bruto.split('\n')
    
   # print(campos_consertando)
    #coloca NAs nos campos vazios
    #for c in range(len(campos_consertando)):
     #   if (campos_consertando[c] == '') & (campos_consertando[c-1] != 'Cargo(s):'):
     #       campos_consertando[c] = "NA"
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
    
    return pesquisa

def pesquisasAntigas():
    lista_links = []
    #pega todos os links antigos
    with open("links_pesquisas.csv", "r", encoding='UTF8') as entrada:
        leitor = csv.reader(entrada)
        for row in leitor:
            lista_links.append(row[1])
        
        #tira apenas os ids
        lista_ids = [l.split("=")[2] for l in lista_links]
        return lista_ids
    
def roda_pesquisa():
    with open("links_pesquisas.csv", "r", encoding='UTF8') as entrada, open("resultado_pesquisas.csv","a",encoding='UTF8') as saida:
        leitor = csv.reader(entrada)
        lista_links = []
        for row in leitor:
            lista_links.append("http://pesqele.tse.jus.br/"+row[1])
        
        resultado = []
        contador = 1
        for t in lista_links:
            print("Checando o link número" + str(contador)+ "...")
            resultado.append(scrape_pagina(t))
            contador += 1

        keys = list(resultado[0].keys())

        dict_writer = csv.DictWriter(saida, keys)
        dict_writer.writer.writerow(keys)
        dict_writer.writerows(resultado)
        
pega_links()

