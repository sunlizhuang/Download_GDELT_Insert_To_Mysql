import requests
import datetime
begin = datetime.date(2014,5,1)#you can change the begin date
end = datetime.date(2014,5,2)#you can change the end date
# these codes will download "Gdelt .csv Data" from begin date to end date
d = begin
delta = datetime.timedelta(days=1)
urls=[]
filePath=[]
while d <= end:
    urls.append("http://data.gdeltproject.org/events/"+d.strftime("%Y%m%d")+".export.CSV.zip")
    filePath.append("./data/"+d.strftime("%Y%m%d")+".export.CSV")
    d += delta


print("downloading....")
for url in urls:
    print("Now_Downloading_url:"+url)
    response = requests.get(url, stream=True)
    with open( './'+url.split('/')[-1], 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)


print("unzip....")
import os
import zipfile
zip_files = [file for file in os.listdir("./") if file.endswith('.zip')]
for zfile in zip_files:
    f = zipfile.ZipFile(os.path.join("./", zfile),'r')
    for file in f.namelist():
        f.extract(file,"./data")





print("insert_to_mysql....")
import pymysql
#@@@@@@@WARNING@@@@@@@@@@@@
# PLEASE! connect to database in your Cmd or Terminal
# code in "mysql -u root -p"
# code in your password
# code in "set global local_infile =1;"
#then you can use these codes to insert your csv files to mysql database

config = {'host': 'localhost',#change by youself
          'port': 3306,
          'user': 'root',#change by youself
          'passwd': '*********',#change by youself
          'charset': 'utf8mb4',
          'local_infile': 1
          }
conn = pymysql.connect(**config)
###create database and table
# cur=conn.cursor()
# cur.execute("create database GDELT_DATA")
# cur.execute("use GDELT_DATA")
# cur.execute("create table data(
#     C1 varchar(100),
#     C2 varchar(100),
#     C3 varchar(100),
#     C4 varchar(100),
#     C5 varchar(100),
#     C6 varchar(100),
#     C7 varchar(100),
#     C8 varchar(100),
#     C9 varchar(100),
#     C10 varchar(100),
#     C11 varchar(100),
#     C12 varchar(100),
#     C13 varchar(100),
#     C14 varchar(100),
#     C15 varchar(100),
#     C16 varchar(100),
#     C17 varchar(100),
#     C18 varchar(100),
#     C19 varchar(100),
#     C20 varchar(100),
#     C21 varchar(100),
#     C22 varchar(100),
#     C23 varchar(100),
#     C24 varchar(100),
#     C25 varchar(100),
#     C26 varchar(100),
#     C27 varchar(100),
#     C28 varchar(100),
#     C29 varchar(100),
#     C30 varchar(100),
#     C31 varchar(100),
#     C32 varchar(100),
#     C33 varchar(100),
#     C34 varchar(100),
#     C35 varchar(100),
#     C36 varchar(100),
#     C37 varchar(100),
#     C38 varchar(100),
#     C39 varchar(100),
#     C40 varchar(100),
#     C41 varchar(100),
#     C42 varchar(100),
#     C43 varchar(100),
#     C44 varchar(100),
#     C45 varchar(100),
#     C46 varchar(100),
#     C47 varchar(100),
#     C48 varchar(100),
#     C49 varchar(100),
#     C50 varchar(100),
#     C51 varchar(100),
#     C52 varchar(100),
#     C53 varchar(100),
#     C54 varchar(100),
#     C55 varchar(100),
#     C56 varchar(100),
#     C57 varchar(100),
#     C58 varchar(100)
# )")


for i in range(len(filePath)):
    cur = conn.cursor()
    data_sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s LINES TERMINATED BY '\n'"%(filePath[i],"GDELT_DATA.data")#change by youself
    # "GDELT_DATA.data" GDELT_DATA is database,data is a table in GDELT_DATA
    print(filePath[i]+"load_in_mysql_finished")
    cur.execute('SET NAMES utf8;')
    cur.execute('SET character_set_connection=utf8;')
    cur.execute(data_sql)
    conn.commit()


