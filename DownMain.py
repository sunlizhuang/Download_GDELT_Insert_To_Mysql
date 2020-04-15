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


for i in range(len(filePath)):
    cur = conn.cursor()
    data_sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s LINES TERMINATED BY '\n'"%(filePath[i],"GDELT_DATA.data")#change by youself
    # "GDELT_DATA.data" GDELT_DATA is my database,data is a table in GDELT_DATA
    print(filePath[i]+"load_in_mysql_finished")
    cur.execute('SET NAMES utf8;')
    cur.execute('SET character_set_connection=utf8;')
    cur.execute(data_sql)
    conn.commit()


