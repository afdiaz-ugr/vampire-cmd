
#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2022,2023,2024 by Antonio Diaz, University of Granada.
# All rights reserved.
# This file is part of the Vampire UGR Power Meter,
# and is released under the "GNU v3 License Agreement". Please see the LICENSE
# file that should have been included as part of this package.


from rx import alias
import serial
import serial.tools.list_ports
import threading
import sys
import getopt
from datetime import datetime,timedelta
import time
import json
import csv

from influxdb_client import InfluxDBClient, WriteOptions, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.util.date_utils import get_date_helper
import warnings
from influxdb_client.client.warnings import MissingPivotFunction

import pandas as pd


warnings.simplefilter("ignore", MissingPivotFunction)
date_helper = get_date_helper()

release="1.2.3"

url=""
xurl = url
org = ""
bucket = ""
xdatabase = ""

user = ""
exp = ""
output_file = ""
measurement = "power"
device = ""
device_list = []
period="5s"
str_time=""
win=False

def get_vclient():
    return InfluxDBClient(url=xurl, token=token,org=org, database=xdatabase)  # debug=True

def _find_getch():
    try:
        import termios
    except ImportError:
        # Non-POSIX. Return msvcrt's (Windows') getch.
        import msvcrt
        return msvcrt.getch

    # POSIX system. Create and return a getch that manipulates the tty.
    import sys, tty
    def _getch():
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return ch

    return _getch

getch = _find_getch()

def Read_ser(ser):
    while 1:
        b = ser.read()
#        print(ord(b))
        if (ord(b)==13):
            print ('\r',end='',flush=True)
        elif (ord(b)==10):
            print ('\r\n',end='',flush=True)
        else:
            print (b.decode(),end='',flush=True)


def Read_kbd(ser):
    while 1:
        k = getch()
        if (ord(k)==3) or (ord(k)==27):
            sys.exit(0)
        if (ord(k))==127:
            k=chr(8)
        if win:
            ser.write(k)
        else:
            ser.write(k.encode())
        if (ord(k)==13):
            ser.write('\n'.encode())


def Vterminal():
    global win
    ports = list(serial.tools.list_ports.comports())
    ser = None
    for p in ports:
        print ("...searching "+p.name)
        if p.description.find("CP210")!=-1:
            print ("Vampire device found in ",p.name)
            if p.name.find("COM")!=-1:
                win=True
            if win:
                ser = serial.Serial(p.name,115200,exclusive=True,xonxoff=0,rtscts=0)
            else:
                ser = serial.Serial("/dev/"+p.name,115200,exclusive=True,xonxoff=0,rtscts=0)
    #        ser.open()
            print (ser.name)
        #ser.setDTR(False)
            ser.flushInput()

    if ser is None:
        print ("No Vampire device found in serial ports")
        sys.exit(0)

        #ser.setDTR(True)
    th1 = threading.Thread(target= Read_ser, args=[ser])
    th2 = threading.Thread(target= Read_kbd, args=[ser])
    try:
        th1.start()
        th2.start()
        th1.join()
        th2.join()
    except KeyboardInterrupt:
        sys.exit(0)


# vampire_cmd help
# vampire_cmd start -u antonio -e exp1
# vampire_cmd stop -u antonio -e exp1
# vampire_cmd info -u antonio -e exp11
# vampire_cmd get -u antonio -e 1 -d m1,m2
# vampire_cmd get -u antonio -e 1 -d m1,m2 -o datos.csv
# vampire_cmd energy -u antonio -e 1 -d m1,m2

def cv_d2i_time(dtime): # convert datetime to influx datetime
    if dtime is None:
        return now().strftime("%Y-%m-%dT%H:%M:%SZ")
    return dtime.strftime("%Y-%m-%dT%H:%M:%SZ")

def set_exp(user, exp, vval,xtime):  # Define_exp_cmd
    vclient = get_vclient()
    write_api = vclient.write_api(write_options=SYNCHRONOUS)
    if xtime is None:
        data = Point("experiment").tag("user", user).tag("exp", exp).field("val", vval)
    else:
        data = Point("experiment").tag("user", user).tag("exp", exp).field("val", vval).time(xtime)
    write_api.write(bucket, org, data)

def List_user_exp_cmd(print_json=False):
    query = f'from(bucket: "{bucket}") \
    |> range(start: -365d) \
    |> filter (fn: (r) => r["_measurement"]=="experiment") \
    |> filter (fn: (r) => r["user"]=="{user}") \
    |> filter (fn: (r) => r["_field"]=="val")'

    vclient = get_vclient()
    result = vclient.query_api().query(query, org=org)

    exp_list = []
    exp_data = {}
    start_time = None
    stop_time = None
    for table in result:
        for record in table.records:
            exp = record.values.get("exp")
            if exp not in exp_list:
                exp_list.append(exp)
                exp_data[exp]={}
                exp_data[exp]["start_time"] = None
                exp_data[exp]["stop_time"] = None
            value = record.get_value()
            if (value==0):
                exp_data[exp]["stop_time"] = record.get_time()
            if (value==1):
                exp_data[exp]["start_time"] = record.get_time()
    print("{[")
    for exp in exp_list:
        x={"user":user,"experiment":exp,"tzname":time.tzname}
        if exp_data[exp]["start_time"] is not None:
            x["start_time"]=utc_to_local(exp_data[exp]["start_time"]).strftime("%Y-%m-%dT%H:%M:%S")
        if exp_data[exp]["stop_time"] is not None:
            x["stop_time"]=utc_to_local(exp_data[exp]["stop_time"]).strftime("%Y-%m-%dT%H:%M:%S")
        print(json.dumps(x),",")
    print("]}")
    return exp_list


def Get_start_stop_time(user,exp):
    query = f'from(bucket: "{bucket}") \
    |> range(start: -365d) \
    |> filter (fn: (r) => r["_measurement"]=="experiment") \
    |> filter (fn: (r) => r["user"]=="{user}") \
    |> filter (fn: (r) => r["exp"]=="{exp}") \
    |> filter (fn: (r) => r["_field"]=="val")'

    vclient = get_vclient()
    result = vclient.query_api().query(query, org=org)
#timestamp = tables[0].records[0]["_stop"]
#print(tables[0].records[0])
    start_time = None
    stop_time = None
    for table in result:
        for record in table.records:
            #print (record)
            value = record.get_value()
            if (value==0):
                stop_time = record.get_time()
            if (value==1):
                start_time = record.get_time()
#            if (value<0):
##            print (record.get_value(), record.get_time())
    return (stop_time, start_time)

def Info_old_exp_cmd():
    (stop_time, start_time) = Get_start_stop_time(user,exp)
    print (stop_time, start_time)
    date_helper = get_date_helper()
    start = date_helper.to_utc(start_time)
    start = "2022-01-11T22:51:24Z"
    print (start)

    query = f'from(bucket: "{bucket}") \
    |> range(start: {start}) \
    |> filter (fn: (r) => r["_measurement"]=="mqtt_consumer") \
    |> filter (fn: (r) => r["user"]=="{user}") \
    |> filter (fn: (r) => r["exp"]=="{exp}") \
    |> filter (fn: (r) => r["_field"]=="power")'

    print (query)
    vclient = get_vclient()
    result = vclient.query_api().query(query, org=org)

    for table in result:
        for record in table.records:
            print (record)
#timestamp = tables[0].records[0]["_stop"]
#print(tables[0].records[0])
#    for table in result:
#        for record in table.records:
#            print (record)


def alias_str():
    xstr=''
    for device in device_list:
        xstr += f'r["alias"]=="{device}" or '
    xstr = xstr[:-4]
    return xstr

def alias_str_1(xdev):
    return f'r["alias"]=="{xdev}"'

def XQueryPD(xfield,wide,xdev):
    (stop_time, start_time) = Get_start_stop_time(user,exp)
    xstart = cv_d2i_time(start_time-timedelta(seconds=wide))
    xstop = cv_d2i_time(stop_time+timedelta(seconds=wide))
    xdata = {}
    #or r["alias"]=="hpm5"
    query = f'from(bucket: "{bucket}") \
    |> range(start: {xstart},stop:{xstop}) \
    |> filter (fn: (r) => r["_measurement"]=="mqtt_consumer") \
    |> filter (fn: (r) => {alias_str_1(xdev)}) \
    |> filter (fn: (r) => r["_field"]=="{xfield}")'
#    |> aggregateWindow(every: {period}, fn: mean)'

    zstart_time = None
    vclient = get_vclient()
    result = vclient.query_api().query_data_frame(query, org=org)
    result.set_index(keys=['_time'], inplace=True)
    result = result[["_value"]]
    pd.set_option('display.max_columns', None)
    result2 = result.resample(period).ffill()
    result2.rename(columns={'_value': xdev}, inplace=True)
#pip    print (result2)
    return result2


def MXQueryPD(xfield,wide):
    df_all = None
    for device in device_list:
        df = XQueryPD(xfield,wide, device)
#        pd.set_option('display.max_rows', None)
#        print (df)
        # combine dataframes
        if device==device_list[0]:
            df_all = df
        else:
            df_all = df_all.join(df, how='outer')
    return df_all



def XQuery(xfield):
    (stop_time, start_time) = Get_start_stop_time(user,exp)
    xstart = cv_d2i_time(start_time)
    xstop = cv_d2i_time(stop_time)
    xdata = {}
    #or r["alias"]=="hpm5"
    query = f'from(bucket: "{bucket}") \
    |> range(start: {xstart},stop:{xstop}) \
    |> filter (fn: (r) => r["_measurement"]=="mqtt_consumer") \
    |> filter (fn: (r) => {alias_str()}) \
    |> filter (fn: (r) => r["_field"]=="{xfield}") \
    |> aggregateWindow(every: {period}, fn: mean)'

    zstart_time = None
    vclient = get_vclient()
    result = vclient.query_api().query(query, org=org)
    for table in result:
        for record in table.records:
            if zstart_time is None:
                zstart_time = record.get_time()
            ztime = int( (record.get_time()-zstart_time).total_seconds())
            zalias = record.values["alias"]
            if ztime not in xdata:
                xdata[ztime]={}
            xdata[ztime][zalias]=record.get_value()
    return xdata


def get_init_info():
    info = {}
    (stop_time, start_time) = Get_start_stop_time(user,exp)
    info["start_time_dt"] = start_time
    info["stop_time_dt"] = stop_time
 #   print (start_time, stop_time)
    if start_time is not None:
        info["start_time"]=utc_to_local(start_time).strftime("%Y-%m-%dT%H:%M:%S.%f")
    if stop_time is not None:
        info["stop_time"]=utc_to_local(stop_time).strftime("%Y-%m-%dT%H:%M:%S.%f")
    return info

def Info_exp_cmd():
    
    first_energy = {}
    last_energy = {}
    info = get_init_info()

    if len(device_list)!=0:
        xdata = XQuery("energy")
        for ztime in sorted(xdata.keys()):
            for zalias in device_list:
                if zalias in xdata[ztime]:
                    if zalias not in first_energy:
                        first_energy[zalias] = xdata[ztime][zalias]
                    if xdata[ztime][zalias] is not None:
                        last_energy[zalias] = xdata[ztime][zalias]
        for zalias in device_list:
            if zalias in last_energy:
                info[zalias] = round(1000.0*(last_energy[zalias]-first_energy[zalias]), 1)
            else:
                info[zalias] = 0

    print(json.dumps(info))
    

# 0 out of window
# 1 in 1st second of window
# 2 full second in window
# 3 in last second of window

def check_type(s_index, info):
    if s_index+timedelta(seconds=1)<info["start_time_dt"]:
        return 0,0
    if s_index>info["stop_time_dt"]:
        return 0,0.0
    if info["start_time_dt"] > s_index and info["start_time_dt"] < s_index+timedelta(seconds=1):
        return 1, (info["start_time_dt"] - s_index).total_seconds()
    if info["stop_time_dt"] > s_index and info["stop_time_dt"] < s_index+timedelta(seconds=1):
        return 3, (info["stop_time_dt"] - s_index).total_seconds()
    return 2,1.0

def calc_exact_energy(xtype,xseg,energy1,energy2):
    if xtype==2:
        return energy1
    diff_energy = (energy2-energy1)/2.0
    partial_diff_energy = (energy2-energy1)*xseg*xseg/2.0
#    print(">",energy1,energy2, diff_energy, partial_diff_energy, xseg)
    #OJO comprobar cuenta
    if xtype==1:
        return energy1+diff_energy-partial_diff_energy
    return energy1+partial_diff_energy

def get_next_row(xdata,s_index):
    idx=xdata.index.get_loc(s_index)
#    print(idx)
    next_row = xdata.iloc[idx+1]
    return next_row


def Energy_data_cmd():
    info = get_init_info()
    xdata = MXQueryPD("power",30)
    energy = {}
    for s_index,row in xdata.iterrows():
        xtype,xseg=check_type(s_index, info)
        if xtype!=0:
            for zalias in device_list:
                if zalias in row:
                    if zalias not in energy:
                        energy[zalias] = 0
                    energy1 = row[zalias]
                    if xtype !=2:
                        next_row=get_next_row(xdata,s_index)
                        energy2 = next_row[zalias]
                    else:
                        energy2 = energy1
                    energy[zalias] += calc_exact_energy(xtype,xseg,energy1,energy2)/3600.0
#                    print(energy1, energy2, calc_exact_energy(xtype,xseg,energy1,energy2))
    for zalias in device_list:
        if zalias in energy:
            info[zalias] = round(1*energy[zalias], 3)
        else:
            info[zalias] = 0
    del info["start_time_dt"]
    del info["stop_time_dt"]
    print(json.dumps(info))


def Get_data_cmd():
    header = "Time,"
    for zalias in device_list:
        header += zalias + ","
    header = header[:-1]
        
    if output_file !="":
        writer=open(output_file, "w")
        writer.write(header+"\n")
    else:
        print(header)
    info = get_init_info()
    xdata = MXQueryPD("power",30)
    ztime = None
    for s_index,row in xdata.iterrows():
        xtype,xseg=check_type(s_index, info)
        if xtype==2:
            if ztime is None:
                ztime = s_index
            result = str( ( s_index-ztime).total_seconds() )+","
            for zalias in device_list:
                if zalias in row:
                    power = row[zalias]
                    result += str("{0:.1f}".format(power)) + ","
            result = result[:-1]
            if output_file=="":
                print(result)
            else:
                writer.write(result+"\n")    


#deprecated
def Get_data_cmd_old():
    header = "Time,"
    for zalias in device_list:
        header += zalias + ","
    header = header[:-1]
        
    if output_file !="":
        writer=open(output_file, "w")
        writer.write(header+"\n")
    else:
        print(header)

    xdata = XQuery("power")
    last_valid_data = {}

    for ztime in sorted(xdata.keys()):
        result = str(ztime) + "," 
        for zalias in device_list:
            if zalias in xdata[ztime] and xdata[ztime][zalias] is not None:
                new_power = xdata[ztime][zalias]
                last_valid_data[zalias] = new_power
#                if xdata[ztime][zalias] is None:
#                    result += "0,"
            else:
                new_power = last_valid_data[zalias]
            result += str("{0:.1f}".format(new_power)) + ","

        result = result[:-1]
        if output_file=="":
            print(result)
        else:
            writer.write(result+"\n")

#            if output_file=="":
#                print ( int( (record.get_time()-zstart_time).total_seconds()) , record.get_value())
#                print (record.values["alias"])
#            else:
#                writer.writerow([int( (record.get_time()-zstart_time).total_seconds()), record.get_value()])
#    if output_file!="":
#        writer.close()

def Getacc_cmd():
    global period
    period = "1s"
    acc_v = {}

    header = "Time,"
    for zalias in device_list:
        header += zalias + ","
    header = header[:-1]
    

    xdata = XQuery("power")

    for ztime in sorted(xdata.keys()):
        result = str(ztime) + "," 
        for zalias in device_list:
            if zalias in xdata[ztime]:
                if xdata[ztime][zalias] is None:
                    result += "0,"
                else:
                    result += str("{0:.1f}".format(xdata[ztime][zalias])) + ","
            else:
                result += "0,"
        result = result[:-1]
        if output_file=="":
            print(result)
        else:
            writer.write(result+"\n")


# current influxdb version does not support delete command with fields
def Delete_exp_cmd_exp(user,exp):
    print ("Current influxdb version does not support delete command")
    return  
    start = "1970-01-01T00:00:00Z"
    stop = "2100-02-01T00:00:00Z"
    #predicate = '_measurement="experiment" AND user=prueba'
    predicate = '_measurement="experiment" AND user="prueba"'
    print(predicate)

#    date_helper = get_date_helper()
#    start = date_helper.to_utc(datetime(1970, 1, 1, 0, 0, 0, 0))
#    stop = date_helper.to_utc(datetime(2200, 1, 1, 0, 0, 0, 0))
    print(start,stop)
    vclient = get_vclient()
    delete_api = vclient.delete_api()
#    delete_api.delete(start, stop, '_measurement="experiment" AND user="antonio" ', bucket=bucket, org=org)
#   delete_api.delete(start, stop, '_measurement="experiment"', bucket=bucket, org=org)
    delete_api.delete(start, stop, predicate=predicate, bucket=bucket, org=org)
#    delete_api.delete(start, stop, '_measurement="experiment" AND user="prueba" AND experiment="z2"', bucket=bucket, org=org)

def ct2i(xtime):
    xt=str(xtime)
    xt=xt.replace(" ", "T")
    xt+="Z"
    return xt

def Delete_exp_cmd(user,exp):
    (stop_time, start_time) = Get_start_stop_time(user,exp)
    print (ct2i(stop_time),ct2i(start_time))
    if stop_time is not None:
        set_exp(user,exp,-10,stop_time)
    if start_time is not None:
        set_exp(user,exp,-9,start_time)   
#    set_exp(user,exp,-10,stop_time)
#    set_exp(user,exp,-9,start_time)

def utc_to_local(dt):
    return dt - timedelta(seconds = time.timezone)

def Define_exp_cmd(user, exp, vval):
    global str_time
    vclient = get_vclient()
    write_api = vclient.write_api(write_options=SYNCHRONOUS)
    if len(str_time)==0:
        data = Point("experiment").tag("user", user).tag("exp", exp).field("val", vval)
    else:
        if str_time[-1]!="Z": # UTC reference time?
            str_time += "Z" # Add UTC reference time
        data = Point("experiment").tag("user", user).tag("exp", exp).field("val", vval).time(str_time)
    write_api.write(bucket, org, data)
    (stop_time, start_time) = Get_start_stop_time(user,exp)

    x={"user":user,"experiment":exp,"tzname":time.tzname}
    if start_time is not None:
        x["start_time"]=utc_to_local(start_time).strftime("%Y-%m-%dT%H:%M:%S")
    if stop_time is not None:
        x["stop_time"]=utc_to_local(stop_time).strftime("%Y-%m-%dT%H:%M:%S")
    print(json.dumps(x))

def check(ck_user=False, ck_exp=False, ck_device=False):
    if ck_user and (user==""):
        print("user is empty")
        sys.exit(1)
    if ck_exp and (exp==""):
        print("exp(experiment) is empty")
        sys.exit(1)
    if ck_device and (device==""):
        print("device is empty")
        sys.exit(1)

def Help_cmd():
    print ("vampire Release:",release)
    print ("vampire.py command [-u,--user <user>] [-e,--exp <exp>] [-d,--device <device_list>] [-t,--time <time>] [-o,--output <output>] [-p,--period <period>]")
    print("""
    Commands:
    help - this help
    start - start experiment
    stop - stop experiment
    info - print experiment info
    energy - get energy consumption
    get - get experiment data
    list - list experiments
    version - version
    """)
    sys.exit(0)


def Main():
    global user, exp, output_file, measurement, device, device_list, period,str_time
    if (len(sys.argv)<2):
        Vterminal()

#    xcmd = "vampire.py energy -u ... -d hpm4,hpm5 -p 1"
#    sysargv =xcmd.split()
#    cmd=sysargv[1]
#    argumentList = sysargv[2:]

    cmd=sys.argv[1]
    if cmd=="-h" or cmd=="--help":
        Help_cmd()
    argumentList = sys.argv[2:]

    options = "u:e:m:d:o:p:t:"
    long_options = ["user=","exp=","measurement=","device=","output=","period","time"]
    try:
        arguments, values = getopt.getopt(argumentList, options, long_options)
        for currentArgument, currentValue in arguments:
            if currentArgument in ("-h", "--help"):
                Help_cmd ()
            elif currentArgument in ("-u", "--user"):
                user = currentValue
            elif currentArgument in ("-e", "--exp"):
                exp = currentValue
            elif currentArgument in ("-o", "--output"):
                output_file = currentValue
            elif currentArgument in ("-m", "--measurement"):
                measurement = currentValue
            elif currentArgument in ("-d", "--device"):
                device = currentValue
                device_list = device.split(',')
            elif currentArgument in ("-t", "--time"):
                str_time = currentValue
            elif currentArgument in ("-p", "--period"):
                period = currentValue
                if period[-1].isnumeric():
                    period += "s"

        if (cmd=="start"):
            check(ck_user=True, ck_exp=True)
            Define_exp_cmd(user,exp,1)
        if (cmd=="stop"):
            check(ck_user=True, ck_exp=True)
            Define_exp_cmd(user,exp,0)
        if (cmd=="energy"):
            check(ck_user=True, ck_exp=True, ck_device=True)
            Energy_data_cmd()
        if (cmd=="info"):
            check(ck_user=True, ck_exp=True)
            Info_exp_cmd()
        if (cmd=="delete"):
            check(ck_user=True, ck_exp=True)
            Delete_exp_cmd(user,exp)
        if (cmd=="list"):
            check(ck_user=True)
            List_user_exp_cmd(print_json=True)
        if (cmd=="get"):
            check(ck_user=True, ck_exp=True, ck_device=True)
            Get_data_cmd()
        if (cmd=="getacc"):
            check(ck_user=True, ck_exp=True, ck_device=True)
            Getacc_cmd()
        if (cmd=="version"):
            print("Release:",release)

        if (cmd=="help"):
            Help_cmd()

    except getopt.error as err:
    # output error, and return with an error code
        print (str(err))

if __name__ == '__main__':
    Main()
