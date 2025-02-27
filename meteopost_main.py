import pandas as pd
import os
import socket
import telebot
import xml.etree.ElementTree as ET
from datetime import datetime
from ftplib import FTP

import meteopost_config as config
import telebot_config

print(telebot_config.token, telebot_config.channel)


## ----------------------------------------------------------------
##  extract year and month  from data
## ----------------------------------------------------------------
def select_year_month(datastring):
    ## '23-02-2025T14:10:00'
    return '_'.join(datastring.split("T")[0].split("-")[2:0:-1]) 


## ----------------------------------------------------------------
##  get filename separator
## ----------------------------------------------------------------
def get_separator():
    if 'ix' in os.name:
        sep = '/'  ## -- path separator for LINIX
    else:
        sep = '\\' ## -- path separator for Windows

    return sep


## ----------------------------------------------------------------
##  
## ----------------------------------------------------------------
def get_local_ip():
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    return hostname, local_ip


## ----------------------------------------------------------------
##  write message to logfile
## ----------------------------------------------------------------
def print_message(message, end=''):
    print(message)
    
    if not message.endswith('\n'):
        end = '\n'
    
    logfilename = logdirname + sep + "_".join(["_".join(str(datetime.now()).split('-')[:2]), device_name,  'log.txt'])       
    with open(logfilename,'a') as flog:
        flog.write(f"{str(datetime.now())}: {message}{end}")


## ----------------------------------------------------------------
##  write message to bot
## ----------------------------------------------------------------
def write_to_bot(text):
    try:
        hostname, local_ip = get_local_ip()
        text = f"{hostname} ({local_ip}): {text}"
        
        bot = telebot.TeleBot(telebot_config.token, parse_mode=None)
        bot.send_message(telebot_config.channel, text)
    except Exception as err:
        ##  напечатать строку ошибки
        text = f" ERROR in writing to bot: {err}"
        print_message(text)  ## write to log file


## ----------------------------------------------------------------
##  download data by ftp
## ----------------------------------------------------------------
def download_data(): 
    if debugmode:
        print("Connecting to ftp...")

    try:
        ftp = FTP()
        ftp.set_debuglevel(level=0)
        ftp.connect(host=config.host, port=config.port)
        ftp.login(user=config.user, passwd=config.passwd)
        #ftp.set_pasv(True)
    except Exception as err:
        ##  напечатать строку ошибки
        text = f"Meteopost: ERROR in ftp downloading: {err}"
        write_to_bot(text)  ## write to log file
        return []

    ## get file list in dir with data
    datafiles = os.listdir(xmldir)

    ## get file list to download
    file_list = ftp.nlst()
    file_list = [file for file in file_list if file not in datafiles]
    
    ##    if debugmode:
    if not file_list:
        write_to_bot("No new files to download from meteopost Chashnikovo!")
    else:
        print_message(f"Found {len(file_list)} files to download")

    ## download files
    if debugmode:
        print("Downloading data from ftp...")
    for file in file_list:
        if debugmode: 
            print(file)
        ftp.retrbinary("RETR " + file, open(f"{xmldir}{sep}{file}", "wb").write)

    ftp.quit()
    return file_list


## ----------------------------------------------------------------
##  dict to rename columns
## ----------------------------------------------------------------
rename = {
    'WS_AVE': 'wind_speed_avr [m/s]', ## скорость ветра [м/с]
    'WS_MAX': 'wind_speed_max [m/s]', ## скорость ветра [м/с]
    'WD'    : 'wind_dir [°]',         ## направление ветра [°]
    'TA_AVE': 'temp_avr [°C]', ##      средняя температура воздуха [°C] за 10 мин на высоте 2м
    'TA_MAX': 'temp_max [°C]', ## максимальная температура воздуха [°C] за 10 мин на высоте 2м
    'TA_MIN': 'temp_min [°C]', ## минимальная  температура воздуха [°C] за 10 мин на высоте 2м
    'RH' : 'humidity [%]',     ## относительная влажность [%]
    'RHG': 'humidity_gnd [%]', ## относительная влажность почвы [%] 
    'AP' : 'pressure [hPa]',        ## атмосферное давление [гПа] “STN” – на уровне станции.
    'AP_MSL': 'pressure_msl [hPa]', ## атмосферное давление [гПа] на уровне моря (Mean Sea Level)
    'PI'   : 'precipitation [mm/h]',   ## Интенсивность осадков [мм/час]
    'P_SUM': 'precipitation_avr [mm]', ## Осадки [мм] суммарные за фиксированный интервал времени осадки. 
    'P_ACC': 'precipitation_acc [mm]', ## Осадки [мм] накопленные осадки за период с фиксированного момента в прошлом
    'LB'   : 'battery [V]',    ## Напряжение батареии [V]
    'TELEM': 'telem [°C]'      ## Температура технологического элемента [°C]
}


## ----------------------------------------------------------------
##  parse one xml file
## ----------------------------------------------------------------
def parse_one_xmlfile(filename):
    root = ET.parse(filename).getroot()
    tabrow = {}

    for tag in ['report']:
        for member in root.iter(tag):
            #record['time'] = member.attrib['TIME']
            tabrow['time'] = member.attrib['TIME']
            ## '23-02-2025T14:10:00'
            tabrow['timestamp'] = int(datetime.strptime(member.attrib['TIME'], "%d-%m-%YT%H:%M:%S").timestamp())
    for tag in ['station']:
        for member in root.iter(tag):
            #print(appt.tag + " ==> " + " ".join(f"{x}: {appt.attrib[x]}" for x in appt.attrib))
            #record[member.tag] = {x: member.attrib[x] for x in member.attrib}
            for x in member.attrib:
                tabrow[f"{member.tag}_{x}"] = member.attrib[x]

    record = {}
    # Поиск тегов <parameter> на любом уровне
    for member in root.iter('parameter'):
        parameter = member.attrib.pop("VAR")
        param = parameter   
        #if member.text.strip() != "":
        #    print(member.tag, parameter, member.attrib, member.text)

        # Перебор детей <value>
        for obj in member.findall('value'):
            param = parameter
            #print(obj.attrib)
            #if param == "TG":
            if "Z" in obj.attrib:
                param = f"{param}_{obj.attrib.pop('Z')}"
            if "PROC" in obj.attrib:
                param = f"{param}_{obj.attrib.pop('PROC')}"
                #print(member.tag, f"{parameter}_{proc}", {**member.attrib, **obj.attrib}, obj.text)
            prop = member.attrib | obj.attrib
            #print(member.tag, param, prop, obj.text)

            # Initialize key if not present in `record`
            if param not in record:
                record[param] = {}

            record[param]["descriptors"] = prop
            record[param]["value"] = obj.text
        
        if parameter == param:
            if param in record:
                param = f"{param}_{member.attrib.pop('Z')}"
            record[param] = {}
            record[param]["descriptors"] = member.attrib
            record[param]["value"] = member.text
    for rec in record:
        if rec in rename:
            tabrow[rename[rec]] = record[rec]["value"]
    #print(tabrow)
    return tabrow ## record


## ----------------------------------------------------------------
##  parse all xml files
## ----------------------------------------------------------------
def parse_xmlfiles(file_list):
    data = []
    if not len(file_list):
        print_message("No files to parse!")
        return data

    for filename in file_list:
        if not filename.endswith(".xml"):
            continue
        record = parse_one_xmlfile(f"{xmldir}{sep}{filename}") 
        data.append(record)    
    return data


## ----------------------------------------------------------------
##  save one xml file
## ----------------------------------------------------------------
def add_data_to_csv_files(dfsave, ym_pattern):
    filename = f"{outdir}{sep}{ym_pattern}_{device_name}.csv"

    ## create new file
    if not os.path.exists(filename):
        dfsave.to_csv(filename, index=False)
        text = f"New {filename} created"
        print_message(text)
        write_to_bot(text)
    else:    
        ## read existing csv file
        dfexist = pd.read_csv(filename)
        dfsave = pd.concat([dfexist, dfsave], ignore_index=True)\
                            .drop_duplicates(subset=['timestamp'])\
                            .sort_values(by=['timestamp'])
        dfsave.to_csv(filename, index=False)    


## ----------------------------------------------------------------
##  save data to files
## ----------------------------------------------------------------
def save_data_to_csv_files(data): 
    ## 
    if data:
        if debugmode: 
            print(data[0])
    else:
        print_message("Empty data converted from xml files!")
        print("exit...")
        return
    
    ## convert to dataframe
    dataframe = pd.DataFrame(data)
    if debugmode: 
        #print(dataframe.head())
        pass

    #### extract year and month from data
    year_month = dataframe['time'].apply(select_year_month).unique()
    if debugmode: 
        print(f"year_month: {year_month}")

    #### write to table files
    for ym_pattern in year_month:
        dfsave = dataframe[dataframe['time'].apply(select_year_month) == ym_pattern]
        #text = ym_pattern + ": " + str(dfsave.shape)
        #print_message(text, '\n')    

        if not dfsave.empty:
            add_data_to_csv_files(dfsave, ym_pattern)


## ----------------------------------------------------------------
## ----------------------------------------------------------------
if __name__ == '__main__':
    debugmode = True
    
    sep = get_separator()
    datadir = f"{config.datadir}"
    xmldir  = f"{datadir}{sep}xml"
    outdir  = f"{datadir}{sep}table"
    logdirname = f"{datadir}{sep}log"
    device_name = "meteopost"

    ## prepare dirs
    for dir in [datadir, xmldir, outdir, logdirname]:
        if not os.path.exists(dir):
            os.mkdir(dir)

    ## download xml files by ftp
    file_list = download_data()
    #file_list = os.listdir(xmldir)

    ## parse xml files
    data = parse_xmlfiles(file_list)
           
    ## save to files
    save_data_to_csv_files(data)
    