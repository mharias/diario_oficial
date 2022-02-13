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
from pass_rss_cncm import apikey_walyt,apisecretkey_walyt,AccessToken_walyt,AccessTokenSecret_walyt,token_bitly,sender_password,sender_email,sender_smtp,path_proyecto,path_proyecto_gcp,path_google_credential ,path_google_credential_gcp

from google.cloud import logging
import html.parser as htmlparser
import bitly_api
import requests
import json
import html
import twitter
import sys

from class_rss_cnmc import RSS_cnmc


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_google_credential_gcp
logging_client = logging.Client()
logger = logging_client.logger('CNMC_RSS')

logger.log_text('Comienza el script',severity='Info')
enlace_rss = 'https://www.cnmc.es/feed/telecomunicaciones'
robot = RSS_cnmc(enlace_rss,apikey_walyt,apisecretkey_walyt,AccessToken_walyt,
              AccessTokenSecret_walyt,sender_email,sender_password,sender_smtp,token_bitly,path_proyecto_gcp)
df = robot.load_rss()
resultado = robot.filtra_resultado(df,desde=1)
mensaje_correo = robot.cuerpo_correo_noticias(resultado,nombre_empresa='')
if resultado.shape[0]!=0:
    robot.send_mail(mensaje_correo,'waly00@gmail.com',port=587)
    robot.publicar_tweets(resultado)