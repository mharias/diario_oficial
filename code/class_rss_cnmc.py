from google.cloud import logging
import xml.etree.ElementTree as ET  
import re
import urllib3
import pandas as pd
import certifi
from datetime import datetime,timedelta
import pytz
import smtplib
import csv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.message import EmailMessage
from email.headerregistry import Address
from email.utils import make_msgid
import sys
import os.path
import numpy as np
sys.path.append("/Users/mharias/Documents/proyectos/mylibs") # directorio de acceso a librerías auxiliares
sys.path.append('/home/waly00/mylibs')
import html.parser as htmlparser
import bitly_api
import requests
import json
import html
import twitter
import sys

class RSS_cnmc():


    def __init__(self,path_rss_,
                 consumer_key_,
               consumer_secret_,
               access_token_key_,
               access_token_secret_,
                sender_email_,
                 sender_password_,
                 sender_smtp_,
                 token_bitly_,
                path_proyecto):

        self.rss = path_rss_
        self.consumer_key=consumer_key_
        self.consumer_secret=consumer_secret_
        self.access_token_key=access_token_key_
        self.access_token_secret=access_token_secret_
        
        self.sender_email = sender_email_
        self.sender_password = sender_password_
        self.sender_server = sender_smtp_
        self.token_bitly = token_bitly_
        
        self.path_proyecto = path_proyecto

        self.fuente = 'Servicio RSS de https://www.esios.ree.es'
        self.autor = '@walyt'
        self.formato_fecha = '%a, %d %b %Y %H:%M:%S %z'
        logging_client = logging.Client()
        self.logger = logging_client.logger('CNMC_RSS')
        self.logger.log_text('Arranca Robot',severity='Info')
        return


    def conversor_fecha(self,string_fecha):
            fecha = datetime.strptime(string_fecha,self.formato_fecha)
            return fecha


    def load_rss(self):
        
        headers = {'Content-type' : 'text/xml',
                   'User-Agent': 'Mozilla/5.0 (X11; Linux i586; rv:31.0) Gecko/20100101 Firefox/31.0'
                    }


        url = self.rss
        http = urllib3.PoolManager()
        response = http.request('GET', url,headers=headers)
        root = ET.fromstring(response.data)
        
        df_historico = pd.DataFrame()
        
        for entrada in root.findall('./channel/item'):
            fecha = entrada.find('./pubDate').text
            titulo = htmlparser.unescape(entrada.find('./title').text)
            link = entrada.find('./link').text
            guid = entrada.find('./guid').text
            fecha = entrada.find('./pubDate').text
            df_historico = df_historico.append({'titulo':titulo,
                                                'enlace':link,
                                                'guid':guid,
                                                'fecha':fecha},
                                                ignore_index=True)
        self.historico = df_historico
        self.logger.log_text('Load RSS :{} resultados'.format(df_historico.shape[0]),severity='Info')
        return df_historico


    def publicar_tweets(self,datos,desde_fecha=''):

        if datos.shape[0]!=0:
            
            self.logger.log_text('Arrancamos API Twitter',severity='Info')
            api=twitter.Api(self.consumer_key,self.consumer_secret,self.access_token_key,self.access_token_secret,sleep_on_rate_limit=True)
            headers = {
                'Authorization': 'Bearer {}'.format(self.token_bitly),
                'Content-Type': 'application/json',
                        }
            self.logger.log_text('API Twitter arrancada',severity='Info')


            for entrada in datos.index:
                data = '{ "long_url":"' + '{}'.format(datos.loc[entrada,'enlace']) + '","domain": "bit.ly"}'
                response = requests.post('https://api-ssl.bitly.com/v4/shorten', headers=headers, data=data)
                enlace_corto = f"http://{json.loads(response.text)['id']}"
                CR='\n'
                titulo = datos.loc[entrada,'titulo']
                text1 = 'Noticias desde CNMC:'
                text2='#cncm #telecomunicaciones'
                text3 = 'Fuente https://www.cnmc.es/ambitos-de-actuacion/telecomunicaciones'
                text3 ='Fuente feed RSS de CNMC Teleco'
                texto=text1+CR+titulo+CR+f'enlace: {enlace_corto}'+CR+text3+CR+text2
                if len(texto)>278:
                    len_titulo = len(titulo)
                    alt_titulo = titulo[:len_titulo-len(texto)+278-3]+'.'*3
                    texto=text1+CR+alt_titulo+CR+f'enlace: {enlace_corto}'+CR+text3+CR+text2
                # print (f'{len(texto)}{texto}{CR*5}')

                estado = api.PostUpdate(texto)
            self.logger.log_text('Terminamos de enviar tuits',severity='Info')  
            return True
        else:
            self.logger.log_text('Resultado vacio, no hay tuits',severity='Info')
        return False
        
    
    def enviar_tweet(self,texto):

        api=twitter.Api(self.consumer_key,self.consumer_secret,self.access_token_key,self.access_token_secret,sleep_on_rate_limit=True)

        estado = api.PostUpdate(texto,media=path_img)
        return estado
    
    
    def cuerpo_correo_noticias(self,df,nombre_empresa=''):
        self.logger.log_text('Comenzamos preparacion Cuerpo mensaje',severity='Info')
        if df.shape[0]!=0:
            rc='<br/>'
            texto=rc+'Buenas tardes:'+rc
            texto+='Resumen de noticias de Telecomunicaciones publicadas en el portal de la CNMC {}'.format('https://www.cnmc.es/ambitos-de-actuacion/telecomunicaciones')
            texto+=rc

            texto+='<p>'
            texto+='*'*10
            texto+=rc
            for i in df.index:

                texto+='<b>Título: </b>'+df.loc[i,'titulo']+rc
                texto+='<b>Enlace :</b> {}'.format(df.loc[i,'enlace'])+rc
                texto+='<p/>'
            # Create the base text message.
            mensaje = EmailMessage()
            mensaje['Subject'] = "Noticias CNMC Telecomunicaciones"
            mensaje['From'] = f'{nombre_empresa} datos'
            mensaje['To'] = Address(f'{nombre_empresa}')

            mensaje.set_content("""
                {text}
                """.format(text=texto))
            asparagus_cid = make_msgid()
            mensaje.add_alternative("""
            <html>
                <head></head>
                <body>
                    <img src="cid:{asparagus_cid}" />
                    <p>
                        {text}
                    </p>

                </body>
            </html>
            """.format(text=texto,asparagus_cid=asparagus_cid[1:-1]), subtype='html')

            path_logo = f'{self.path_proyecto}logos/{nombre_empresa}.png'

            if os.path.isfile(path_logo):
                with open(path_logo, 'rb') as img:
                    mensaje.get_payload()[1].add_related(img.read(), 'image', 'png',
                                             cid=asparagus_cid)
            self.logger.log_text('Cuerpo mensaje preparado',severity='Info')
            return mensaje
        else:
            self.logger.log_text('Mensaje vacío',severity='Info')
            return False
    
    def filtra_resultado(self, df, desde=1):
        local = pytz.timezone('UTC')
        hora_tope = local.localize(datetime.utcnow(),is_dst=None)-timedelta(days=desde)
        
        if df.shape[0]!=0:
            resultado = (df.
                         loc[df.apply(lambda x: datetime.strptime(x['fecha'],self.formato_fecha) > hora_tope,axis=1),:])
            self.logger.log_text('Filtramos resultados: {}'.format(resultado.shape[0]),severity='Info')
            return resultado
        self.logger.log_text('Filtramos resultados: ya esta vacío',severity='Info')
        return df
        
    
    
    def send_mail(self,mensaje,lista,port=587):
        if int(port) == 465:    # gmail server
            email_server = smtplib.SMTP_SSL(self.sender_server, str(port))
        else:
            email_server = smtplib.SMTP(self.sender_server, port)
            email_server.ehlo()
            email_server.starttls()
        email_server.login(self.sender_email,self.sender_password)
        email_server.sendmail(self.sender_email,lista,mensaje.as_string())
        email_server.quit()
        self.logger.log_text('Enviamos email',severity='Info')
        