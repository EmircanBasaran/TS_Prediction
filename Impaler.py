from pycaret.anomaly import *
import pandas as pd
import datetime as dt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
import datetime as dt
import logging
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

currentTime = dt.datetime.now()
logging.warning('Impaler analizi basladi.')

#Read Excel file via openpyxl
csvPath = '/usr/src/app/data/csv/'+str(sys.argv[1])+'/'
data=pd.read_excel(csvPath+"ConsolideExcel.xlsx",sheet_name='jobDetails',engine='openpyxl')
data=data[['StartTime','GramPerSecond']]
data=data[data['StartTime']<'2021-06-11']
data.rename(columns={'StartTime':'ds','GramPerSecond':'y'},inplace=True)
data = data.resample('D',on='ds').mean()
data.reset_index(inplace=True) 
#data.dropna(inplace=True)
data.interpolate(inplace=True)
data['ds']=pd.to_datetime(data['ds'])
data['ds']=data['ds'].map(dt.datetime.toordinal)

#Anomaly detection and interpolation
logging.warning('Anomaly tespiti basladi')
s = setup(data,categorical_features=['ds'],numeric_features=['y'],silent=True)
iforest = create_model('iforest', fraction = 0.1)
iforest_results = assign_model(iforest)
data=iforest_results[iforest_results['Anomaly'] == 0]
data.interpolate(inplace=True)

#Polynomial Regression prediction
logging.warning('Gelecek haftaya yonelik tahmin hazirlaniyor')
poly = PolynomialFeatures(degree = 3)
X_poly = poly.fit_transform(data['ds'].values.reshape(-1,1))
poly.fit(X_poly, data['y'])
lin2 = LinearRegression()
lin2.fit(X_poly, data['y'])

#4 cases for fault tolarence
if data[-7::]['y'].mean()<200:
  print('average<200 YES')
if (data['y'].tail(1)<200).any():
  print('last day<200 YES')
if (lin2.predict(poly.fit_transform((data[-7::]['ds']+7).values.reshape(-1,1)))[6]-lin2.predict(poly.fit_transform((data[-7::]['ds']+7).values.reshape(-1,1)))[0])/7 <0:
  print('- olarak dusuyor YES')
if (len(data)>30):
    print('30 gunden fazla YES')

logging.warning('Impaler analizi bitti.')
logging.warning('Toplam sure: '+ str(dt.datetime.now() - currentTime))

#total case with if clause and html context to send warning mail
if (data[-7::]['y'].mean()<200) & ((data['y'].tail(1)<200).any()) & ((lin2.predict(poly.fit_transform((data[-7::]['ds']+7).values.reshape(-1,1)))[6]-lin2.predict(poly.fit_transform((data[-7::]['ds']+7).values.reshape(-1,1)))[0])/7 <0) & (len(data)>30):
    sender_email = "emircanbasaran1@gmail.com"
    receiver_email = "emircan.basaran@eliarge.com"
    password = ''

    message = MIMEMultipart("alternative")
    message["Subject"] = "Impaler Arizasi !!"
    message["From"] = sender_email
    message["To"] = receiver_email

    html = """<h2><span style="color: #ff0000;"><strong>IMPALER ARIZASI ! </strong></span></h2>
    <h2><span style="color: #ff0000;"><strong>POMPA YAKIN ZAMANDA BOZULACAKTIR LUTFEN TEKNIK SERVISLE ILETISIME GECINIZ !</strong></span></h2>
    <p>&nbsp;</p>
    <h4><span style="color: #ff0000;"><strong>TEKNIK SERVIS NO:</strong></span></h4>
    <h4><span style="color: #ff0000;"><strong>HATA KODU:</strong></span></h4>
    <p><img src="https://eliar.com/wp-content/uploads/2018/12/eliarkurumsal.png" alt="ELIAR ELEKTRONIK" width="605" height="194" /></p>
    """

    #part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")
    #message.attach(part1)
    message.attach(part2)

    #Create secure connection with server and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(
            sender_email, receiver_email, message.as_string()
        )