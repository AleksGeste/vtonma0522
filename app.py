import schedule
import requests
import smtplib
import datetime
import os, time
import json
import socket

import gspread

from oauth2client.service_account import ServiceAccountCredentials

from bs4 import BeautifulSoup
from datetime import date
from email.message import EmailMessage

scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file',
 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('python-save-meteo-data.json', scope)
client = gspread.authorize(creds)

nma_list = client.open("VTO_Meteo_App_NMA").worksheet("NMA_List")
data_from_vbp_meteo = client.open("VTO_Meteo_App").worksheet("dataFromVbpMeteo")
current_nma_count = client.open("currentNmaCount_list").worksheet("currentNmaCount")


URL = 'https://api.met.no/weatherapi/locationforecast/2.0/compact'

def save_nma(nma):
    nma_list.append_row(nma, 'RAW', None, 'A1:E1')

def save_data_from_vbp_meteo(data1):
    data_from_vbp_meteo.append_row(data1, 'RAW', None, 'A1:D1')

def set_nma_count_g_sheet(new_number, pk):
    if pk == 'pk1':
        current_nma_count.update_acell('A2', new_number)
    elif pk == 'pk2':
        current_nma_count.update_acell('B2', new_number)
    elif pk == 'pk3':
        current_nma_count.update_acell('C2', new_number)

def get_nma_count_g_sheet(pk):
    if pk == 'pk1':
        return current_nma_count.acell('A2').value
    elif pk == 'pk2':
        return current_nma_count.acell('B2').value
    elif pk == 'pk3':
        return current_nma_count.acell('C2').value

def set_nma_status_g_sheet(status, pk):
    if pk == 'pk1':
        current_nma_count.update_acell('A3', status)
    elif pk == 'pk2':
        current_nma_count.update_acell('B3', status)
    elif pk == 'pk3':
        current_nma_count.update_acell('C3', status)

def get_nma_status_g_sheet(pk):
    if pk == 'pk1':
        if current_nma_count.acell('A3').value == '1':
            return True
        else:
            return False
    elif pk == 'pk2':
        if current_nma_count.acell('B3').value == '1':
            return True
        else:
            return False
    elif pk == 'pk3':
        if current_nma_count.acell('C3').value == '1':
            return True
        else:
            return False

def get_dma_pk_1_cout():
    return int(get_nma_count_g_sheet('pk1'))

def get_dma_pk_2_cout():
    return int(get_nma_count_g_sheet('pk2'))

def get_dma_pk_3_cout():
    return int(get_nma_count_g_sheet('pk3'))

def get_dma_pk_1():
    return get_nma_status_g_sheet('pk1')

def get_dma_pk_2():
    return get_nma_status_g_sheet('pk2')

def get_dma_pk_3():
    return get_nma_status_g_sheet('pk3')

def set_dma_pk_1(value):
    if value:
        set_nma_status_g_sheet('1', 'pk1')
    else:
        set_nma_status_g_sheet('0', 'pk1')

def set_dma_pk_2(value):
    if value:
        set_nma_status_g_sheet('1', 'pk2')
    else:
        set_nma_status_g_sheet('0', 'pk2')

def set_dma_pk_3(value):
    if value:
        set_nma_status_g_sheet('1', 'pk3')
    else:
        set_nma_status_g_sheet('0', 'pk3')

def dma_pk_1_cout_increment():
    num = int(get_nma_count_g_sheet('pk1'))
    num = num + 1
    set_nma_count_g_sheet(num, 'pk1')

def dma_pk_2_cout_increment():
    num = int(get_nma_count_g_sheet('pk2'))
    num = num + 1
    set_nma_count_g_sheet(num, 'pk2')

def dma_pk_3_cout_increment():
    num = int(get_nma_count_g_sheet('pk3'))
    num = num + 1
    set_nma_count_g_sheet(num, 'pk3')

def setTime(hr, min=0, sec=0, micros=0):
   now = datetime.datetime.now()
   return now.replace(hour=hr, minute=min, second=sec, microsecond=micros)


def getWeather():
    querystring = {"lat":"57.39961155153841","lon":"21.564216534621924"}
    payload = ""
    headers = {"User-Agent": "a.cikinovs@icloud.com"}
    response = requests.request("GET", URL, data=payload, headers=headers, params=querystring)
    y = json.loads(response.text)
    return y

# main implementation function
def start_dma(speed, direction):
    print('Start - start_dma()')
    print('Direction is: ', str(direction))

    if speed > 7:
        print('Start - if wind speed > 7')
        print('Direction is: ', str(direction))

#         if direction == "Z":
        if direction > 270 and direction < 125:

            print('Start - if direction > 270 and direction < 125')
            print("It is Z. And It is PK-1")

            if get_dma_pk_1() != True:
                print('Start - if get_dma_pk_1() != True')
                set_dma_pk_1(True)
                dma_pk_1_cout_increment()
                dateIs = current_date()
                timeIs = current_time()
                bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 3-" + current_month() + "/" + str(get_dma_pk_1_cout()) + "\n" + "Iestājušies DMA - Vēja virziens 270° - 125°; Vēja ātrums virs 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
                print(bodyIs)
                send_emails("DMA izmaiņas terminālī Nr.1", bodyIs)
                data = ['PK-1', current_date(), current_time(), "3-" + current_month() + "/" + str(get_dma_pk_1_cout()), 'Iestājušies']
                save_nma(data)
                print('End - if get_dma_pk_1() != True')

            if get_dma_pk_2():
                print('Start - if get_dma_pk_2()')
                set_dma_pk_2(False)
                dateIs = current_date()
                timeIs = current_time()
                bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 4-" + current_month() + "/" + str(get_dma_pk_2_cout()) + "\n" + "Izbeigušies DMA - Vēja virziens 325° - 55°; Vēja ātrums zem 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
                print(bodyIs)
                send_emails("DMA izmaiņas terminālī Nr.2", bodyIs)
                data = ['PK-2', current_date(), current_time(), "4-" + current_month() + "/" + str(get_dma_pk_2_cout()), 'Izbeigušies']
                save_nma(data)
                print('End - if get_dma_pk_2()')

            if get_dma_pk_3():
                print('Start - if get_dma_pk_3()')
                set_dma_pk_3(False)
                dateIs = current_date()
                timeIs = current_time()
                bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 2-" + current_month() + "/" + str(get_dma_pk_3_cout()) + "\n" + "Izbeigušies DMA - Vēja virziens 10° - 90° vai 150° - 320°; Vēja ātrums zem 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
                print(bodyIs)
                send_emails("DMA izmaiņas terminālī Nr.3", bodyIs)
                data = ['PK-3', current_date(), current_time(), "2-" + current_month() + "/" + str(get_dma_pk_3_cout()), 'Izbeigušies']
                save_nma(data)
                print('End - if get_dma_pk_3()')

            print('End - if direction == "Z"')

#         elif direction == "Z-A":
        elif direction > 20 and direction < 70:
            print('Start - elif direction > 20 and direction < 70')
            print("It is Z-A. And It is PK-1 and PK-3")

            if get_dma_pk_1() != True:
                print('Start - if get_dma_pk_1() != True')
                set_dma_pk_1(True)
                dma_pk_1_cout_increment()
                dateIs = current_date()
                timeIs = current_time()
                bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 3-" + current_month() + "/" + str(get_dma_pk_1_cout()) + "\n" + "Iestājušies DMA - Vēja virziens 270° - 125°; Vēja ātrums virs 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
                print(bodyIs)
                send_emails("DMA izmaiņas terminālī Nr.1", bodyIs)
                data = ['PK-1', current_date(), current_time(), "3-" + current_month() + "/" + str(get_dma_pk_1_cout()), 'Iestājušies']
                save_nma(data)
                print('End - if get_dma_pk_1() != True')

            if get_dma_pk_3() != True:
                print('Start - if get_dma_pk_3() != True')
                set_dma_pk_3(True)
                dma_pk_3_cout_increment()
                dateIs = current_date()
                timeIs = current_time()
                bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 2-" + current_month() + "/" + str(get_dma_pk_3_cout()) + "\n" + "Iestājušies DMA - Vēja virziens 10° - 90° vai 150° - 320°; Vēja ātrums virs 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
                print(bodyIs)
                send_emails("DMA izmaiņas terminālī Nr.3", bodyIs)
                data = ['PK-3', current_date(), current_time(), "2-" + current_month() + "/" + str(get_dma_pk_3_cout()), 'Iestājušies']
                save_nma(data)
                print('End - if get_dma_pk_3() != True')

            if get_dma_pk_2():
                print('Start - if get_dma_pk_2()')
                set_dma_pk_2(False)
                dateIs = current_date()
                timeIs = current_time()
                bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 4-" + current_month() + "/" + str(get_dma_pk_2_cout()) + "\n" + "Izbeigušies DMA - Vēja virziens 325° - 55°; Vēja ātrums zem 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
                print(bodyIs)
                send_emails("DMA izmaiņas terminālī Nr.2", bodyIs)
                data = ['PK-2', current_date(), current_time(), "4-" + current_month() + "/" + str(get_dma_pk_2_cout()), 'Izbeigušies']
                save_nma(data)
                print('Start - if get_dma_pk_2()')

            print('End - elif direction == "Z-A"')

#         elif direction == "D-R":
        elif direction > 200 and direction < 250:
            print('Start - elif direction > 200 and direction < 250')
            print("It is D-R. And It is PK-3")

            if get_dma_pk_3() != True:
                print('Start - if get_dma_pk_3() != True')
                set_dma_pk_3(True)
                dma_pk_3_cout_increment()
                dateIs = current_date()
                timeIs = current_time()
                bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 2-" + current_month() + "/" + str(get_dma_pk_3_cout()) + "\n" + "Iestājušies DMA - Vēja virziens 10° - 90° vai 150° - 320°; Vēja ātrums virs 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
                print(bodyIs)
                send_emails("DMA izmaiņas terminālī Nr.3", bodyIs)
                data = ['PK-3', current_date(), current_time(), "2-" + current_month() + "/" + str(get_dma_pk_3_cout()), 'Iestājušies']
                save_nma(data)
                print('End - if get_dma_pk_3() != True')

            if get_dma_pk_2():
                print('Start - if get_dma_pk_2()')
                set_dma_pk_2(False)
                dateIs = current_date()
                timeIs = current_time()
                bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 4-" + current_month() + "/" + str(get_dma_pk_2_cout()) + "\n" + "Izbeigušies DMA - Vēja virziens 325° - 55°; Vēja ātrums zem 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
                print(bodyIs)
                send_emails("DMA izmaiņas terminālī Nr.2", bodyIs)
                data = ['PK-2', current_date(), current_time(), "4-" + current_month() + "/" + str(get_dma_pk_2_cout()), 'Izbeigušies']
                save_nma(data)
                print('End - if get_dma_pk_2()')

            if get_dma_pk_1():
                print('Start - if get_dma_pk_1()')
                set_dma_pk_1(False)
                dateIs = current_date()
                timeIs = current_time()
                bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 3-" + current_month() + "/" + str(get_dma_pk_1_cout()) + "\n" + "Izbeigušies DMA - Vēja virziens 270° - 125°; Vēja ātrums zem 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
                print(bodyIs)
                send_emails("DMA izmaiņas terminālī Nr.1", bodyIs)
                data = ['PK-1', current_date(), current_time(), "3-" + current_month() + "/" + str(get_dma_pk_1_cout()), 'Izbeigušies']
                save_nma(data)
                print('End - if get_dma_pk_1()')

            print('End - elif direction == "D-R"')

#         elif direction == "D":
        elif direction > 170 and direction < 190:
            print('Start - elif direction > 170 and direction < 190')
            print("It is D. And It is PK-2")

            if get_dma_pk_2() != True:
                print('Start - if get_dma_pk_2() != True')
                set_dma_pk_2(True)
                dma_pk_2_cout_increment()
                dateIs = current_date()
                timeIs = current_time()
                bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 4-" + current_month() + "/" + str(get_dma_pk_2_cout()) + "\n" + "Iestājušies DMA - Vēja virziens 325° - 55°; Vēja ātrums virs 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
                print(bodyIs)
                send_emails("DMA izmaiņas terminālī Nr.2", bodyIs)
                data = ['PK-2', current_date(), current_time(), "4-" + current_month() + "/" + str(get_dma_pk_2_cout()), 'Iestājušies']
                save_nma(data)
                print('End - if get_dma_pk_2() != True')

            if get_dma_pk_1():
                print('Start - if get_dma_pk_1()')
                set_dma_pk_1(False)
                dateIs = current_date()
                timeIs = current_time()
                bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 3-" + current_month() + "/" + str(get_dma_pk_1_cout()) + "\n" + "Izbeigušies DMA - Vēja virziens 270° -125°; Vēja ātrums zem 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
                print(bodyIs)
                send_emails("DMA izmaiņas terminālī Nr.1", bodyIs)
                data = ['PK-1', current_date(), current_time(), "3-" + current_month() + "/" + str(get_dma_pk_1_cout()), 'Izbeigušies']
                save_nma(data)
                print('End - if get_dma_pk_1()')

            if get_dma_pk_3():
                print('Start - if get_dma_pk_3()')
                set_dma_pk_3(False)
                dateIs = current_date()
                timeIs = current_time()
                bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 2-" + current_month() + "/" + str(get_dma_pk_3_cout()) + "\n" + "Izbeigušies DMA - Vēja virziens 10° - 90° vai 150° - 320°; Vēja ātrums zem 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
                print(bodyIs)
                send_emails("DMA izmaiņas terminālī Nr.3", bodyIs)
                data = ['PK-3', current_date(), current_time(), "2-" + current_month() + "/" + str(get_dma_pk_3_cout()), 'Izbeigušies']
                save_nma(data)
                print('End - if get_dma_pk_1()')

            print('End - elif direction == "D"')

        else:
            # wind is more than 5 but direction is ok.
            print("Start - Wind is more than 7, but direction is OK!")
            all_state_to_off()
            print("Start - Wind is more than 7, but direction is OK!")

        print('End - if wind speed > 7')

    elif speed < 3.5:
        print('Start - elif wind speed < 3.5')
        print("Wind is less then 3.5")
        all_state_to_off()
        print('End - elif wind speed < 3.5')

#     elif direction == "A":
    elif direction > 80 and direction < 100:
        print('Start - elif direction == "A"')
        all_state_to_off()
        print('End - elif direction == "A"')

    print('End - start_dma()')

def all_state_to_off():
    if get_dma_pk_1():
        print('Start - if get_dma_pk_1()')
        set_dma_pk_1(False)
        dateIs = current_date()
        timeIs = current_time()
        bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 3-" + current_month() + "/" + str(get_dma_pk_1_cout()) + "\n" + "Izbeigušies DMA - Vēja virziens 270° -125°; Vēja ātrums zem 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
        print(bodyIs)
        send_emails("DMA izmaiņas terminālī Nr.1", bodyIs)
        data = ['PK-1', current_date(), current_time(), "3-" + current_month() + "/" + str(get_dma_pk_1_cout()), 'Izbeigušies']
        save_nma(data)
        print('End - if get_dma_pk_1()')

    if get_dma_pk_2():
        print('Start - if get_dma_pk_2()')
        set_dma_pk_2(False)
        dateIs = current_date()
        timeIs = current_time()
        bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 4-" + current_month() + "/" + str(get_dma_pk_2_cout()) + "\n" + "Izbeigušies DMA - Vēja virziens 325° - 55°; Vēja ātrums zem 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
        print(bodyIs)
        send_emails("DMA izmaiņas terminālī Nr.2", bodyIs)
        data = ['PK-2', current_date(), current_time(), "4-" + current_month() + "/" + str(get_dma_pk_2_cout()), 'Izbeigušies']
        save_nma(data)
        print('End - if get_dma_pk_2()')

    if get_dma_pk_3():
        print('Start - if get_dma_pk_3()')
        set_dma_pk_3(False)
        dateIs = current_date()
        timeIs = current_time()
        bodyIs = "\n" + dateIs + " " + timeIs + "\n" + "\n" + "Nr. 2-" + current_month() + "/" + str(get_dma_pk_3_cout()) + "\n" + "Izbeigušies DMA - Vēja virziens 10° - 90° vai 150° - 320°; Vēja ātrums zem 5 m/s" + "\n" + "Šis ir automātisks paziņojums, lūdzam uz to neatbildēt."
        print(bodyIs)
        send_emails("DMA izmaiņas terminālī Nr.3", bodyIs)
        data = ['PK-3', current_date(), current_time(), "2-" + current_month() + "/" + str(get_dma_pk_3_cout()), 'Izbeigušies']
        save_nma(data)
        print('End - if get_dma_pk_3()')

# email sending function
def email_alert(to, subject, body):
    msg = EmailMessage()
    msg['to'] = to
    msg['subject'] = subject
    msg.set_content(body)

    user = "info.devoncouch@gmail.com"
    msg['from'] = "VTO Meteo Alert"
    password = "htsnxgjvpnavjlxd"

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(user, password)
    server.send_message(msg)
    server.quit

# send emails
def send_emails(subject, body):
#     email_alert("a.cikinovs@icloud.com", subject, body)
    email_alert("mainas.prieksnieksPK1@vto.lv", subject, body)
    email_alert("alina.rugine@vto.lv", subject, body)
    email_alert("mainas.prieksnieksPK2@vto.lv", subject, body)
    email_alert("aleksandrs.cikinovs@gmail.com", subject, body)
    email_alert("nadezda.fjodorova@vto.lv", subject, body)
    email_alert("vjaceslavs.krotovs@vto.lv", subject, body)

# date and time detection function
def current_date():
    os.environ['TZ'] = 'Europe/Riga'
    time.tzset()
    year = time.strftime('%Y')
    month = time.strftime('%m')
    day = time.strftime('%d')
    return day + "." + month + "." + year

def current_time():
    os.environ['TZ'] = 'Europe/Riga'
    time.tzset()
    hour = time.strftime('%H')
    minute = time.strftime('%M')
    second = time.strftime('%S')
    return hour + ":" + minute + ":" + second

def current_month():
    os.environ['TZ'] = 'Europe/Riga'
    time.tzset()
    month = time.strftime('%m')
    return month

def reset_nma_count():
    cur_month = int(current_nma_count.acell('E2').value)
    if cur_month < int(current_month()):
        if not get_dma_pk_1():
            set_nma_count_g_sheet('0', 'pk1')
        if not get_dma_pk_2():
            set_nma_count_g_sheet('0', 'pk2')
        if not get_dma_pk_3():
            set_nma_count_g_sheet('0', 'pk3')

def job():
    weatherData = getWeather()
    updated_at = weatherData["properties"]["meta"]["updated_at"]
    print("------------------------------")
    print("Updated at: " + updated_at)

    wind_speed = weatherData["properties"]["timeseries"][0]["data"]["instant"]["details"]["wind_speed"]
    wind_direction = weatherData["properties"]["timeseries"][0]["data"]["instant"]["details"]["wind_from_direction"]

    print("wind_speed: " + str(wind_speed))
    print("wind_direction: " + str(wind_direction))


    print("I'm working...")
    reset_nma_count()
    current_nma_count.update_acell('E2', current_month())
    print("... is UP! Date is: " + current_date() + " Time is: " + current_time() + " Dir: " + str(wind_direction) + " Speed: " + str(wind_speed))
    data_to_send = [current_date(), current_time(), str(wind_direction), str(wind_speed)]
    save_data_from_vbp_meteo(data_to_send)
    start_dma(wind_speed, wind_direction)

if __name__ == '__main__':
    while(True):
        job()
        time.sleep(9000)
#         time.sleep(10)
