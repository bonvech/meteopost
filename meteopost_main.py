from ftplib import FTP
import os
import meteopost_config as config


datadir = config.datadir

print("ftp programm")

ftp = FTP()
ftp.set_debuglevel(level=0)
ftp.connect(host=config.host, port=config.port)
ftp.login(user=config.user, passwd=config.passwd)
#ftp.set_pasv(True)

## get file list in data
datafiles = os.listdir("data")

## get file list
file_list = ftp.nlst()
file_list = [file for file in file_list if file not in datafiles]
#print(len(file_list))

## download files
for file in file_list:
    print(file)
    ftp.retrbinary("RETR " + file, open("data/" + file, "wb").write)

ftp.quit()
