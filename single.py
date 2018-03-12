# -*- coding: utf-8 -*-

from pyhive import hive
from TCLIService.ttypes import TOperationState
import pandas as pd
import numpy as np
from datetime import datetime
import time
from pandas import Series, DataFrame


starttime = datetime.now()
mon = '201801'
da = list(i for i in range(31, 32))


def ts(x=1):
    """

    :param x:
    :return:
    """
    if len(str(x)) == 1:
        return '0'+str(x)
    else:
        return str(x)


db = map(ts, da)

def ti(x):
    """

        :param x:
        :return:
        """
    x = datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
    x = x.strftime("%H:%M:%S")
    return x


for e in db:
    hql = 'select pdevicetype,from_unixtime (CAST (ts/ 1000 AS BIGINT),"yyyy-MM-dd HH:mm:ss") ts,from_unixtime (CAST (ts/ 1000 AS BIGINT),"yyyy-MM-dd") dd,macid,argsext["60600B"] f60600B,argsext["606006"] f606006,argsext["60600t"] f60600t,argsext["606007"] f606007,argsext["606005"] f606005,argsext["606001"] f606001,argsext["606004"] f606004 from ods_kafka_device_datareportinfo where pdevicetype="06012004"  and pdate = %s%s%s and pday = %s%s%s and name = "sendDeviceStatus" and argsext["60600B"]>0 and macid ="DC330D4AC835"'% ('"', mon, '"', '"', e, '"')
    hiveConn = hive.connect(host="ip address", port=10000, username="uname", database='dname')
    cursor = hiveConn.cursor()
    cursor.execute(hql)
    cls = ['pdevicetype', 'ts', 'dd', 'macid', 'f60600b', 'f606006', 'f60600t', 'f606007', 'f606005', 'f606001', 'f606004']
    status = cursor.poll().operationState
    dt = cursor.fetchall()
    df = pd.DataFrame(dt, columns=cls)
    df['f60600b'] = [float(i) for i in df['f60600b']]
    df['f606006'] = [float(i) for i in df['f606006']]
    df['f606007'] = [float(i) for i in df['f606007']]
    df['f606001'] = [float(i) for i in df['f606001']]
    clss = ['macid', 'dd', 'bt', 'et', 'ysl']
    clsss = ['macid', 'dd', 'ks', 'js', 'sl']
    dz = pd.DataFrame(columns=clsss)
    for i in range(len(df['f606007'])):
        if(df['f606007'][i] < 40):
            df.loc[i, ['f606007']] = 40
        if (df['f606006'][i] >= 40):
            df.loc[i, ['f606006']] = 15
    dmd = df['macid'].drop_duplicates()
    for aa in dmd:
        ds = df[df['macid'] == aa]
        ds = ds.sort_values(by='ts')
        it = Series(i for i in range(len(ds)))
        ds = DataFrame(ds.values, index=it, columns=ds.columns)
        dd = []
        bt = []
        et = []
        ysl = []
        mac = []
        ks = []
        js = []
        sl = []
        macid = []
        ddd = []
        da = pd.DataFrame(columns=clss)
        dh = pd.DataFrame(columns=clsss)
        if(len(ds)>=200):
            for i in range(len(ds)):
                if(i == 0):
                    bt.append(ds['ts'][i])
                if(i==0 and (datetime.strptime(ti(ds['ts'][i + 1]), "%H:%M:%S") - datetime.strptime(ti(ds['ts'][i]),"%H:%M:%S")).seconds > 10):
                    et.append(ds['ts'][i])
                if((i>0 and i<len(ds)-1) and (datetime.strptime(ti(ds['ts'][i]), "%H:%M:%S") - datetime.strptime(ti(ds['ts'][i-1]),"%H:%M:%S")).seconds > 10):
                    bt.append(ds['ts'][i])
                if((i>0 and i<len(ds)-1) and (datetime.strptime(ti(ds['ts'][i + 1]), "%H:%M:%S") - datetime.strptime(ti(ds['ts'][i]),"%H:%M:%S")).seconds > 10):
                    et.append(ds['ts'][i])
                if(i==len(ds)-1 and (datetime.strptime(ti(ds['ts'][i]), "%H:%M:%S") - datetime.strptime(ti(ds['ts'][i-1]),"%H:%M:%S")).seconds > 10):
                    bt.append(ds['ts'][i])
                if(i == len(ds)-1):
                    et.append(ds['ts'][i])
        if(len(ds) >= 200):
            for i in range(len(ds)):
                if (i == 0):
                    ks.append(ds['ts'][i])
                    macid.append(aa)
                    ddd.append(mon + e)
                if (i == 0 and (datetime.strptime(ti(ds['ts'][i + 1]), "%H:%M:%S") - datetime.strptime(ti(ds['ts'][i]), "%H:%M:%S")).seconds > 180):
                    js.append(ds['ts'][i])
                if ((i > 0 and i < len(ds) - 1) and (datetime.strptime(ti(ds['ts'][i]), "%H:%M:%S") - datetime.strptime(ti(ds['ts'][i - 1]), "%H:%M:%S")).seconds > 180):
                    ks.append(ds['ts'][i])
                    macid.append(aa)
                    ddd.append(mon + e)
                if ((i > 0 and i < len(ds) - 1) and (datetime.strptime(ti(ds['ts'][i + 1]), "%H:%M:%S") - datetime.strptime(ti(ds['ts'][i]), "%H:%M:%S")).seconds > 180):
                    js.append(ds['ts'][i])
                if (i == len(ds) - 1 and (datetime.strptime(ti(ds['ts'][i]), "%H:%M:%S") - datetime.strptime(ti(ds['ts'][i - 1]), "%H:%M:%S")).seconds > 180):
                    ks.append(ds['ts'][i])
                    macid.append(aa)
                    ddd.append(mon + e)
                if (i == len(ds) - 1):
                    js.append(ds['ts'][i])
        dh['macid'] = macid
        dh['ks'] = ks
        dh['js'] = js
        dh['dd'] = ddd


        for i in range(len(bt)):
            sum = 0
            for j in range(len(ds)):
                if((j<len(ds)-1) and bt[i]<et[i] and ds['ts'][j]>=bt[i] and ds['ts'][j+1]<=et[i]):
                    sum += (datetime.strptime(ti(ds['ts'][j + 1]), "%H:%M:%S") - datetime.strptime(ti(ds['ts'][j]),"%H:%M:%S")).seconds / 60.0 * ds['f60600b'][j] * (1 + (ds['f606007'][j] - 40.0) / (40.0 - ds['f606006'][j]))
                if(bt[i]==et[i] and ds['ts'][j]==bt[i]):
                    sum += 1 / 60.0 * ds['f60600b'][j] * (1 + (ds['f606007'][j] - 40.0) / (40.0 - ds['f606006'][j]))
                if (bt[i] < et[i] and ds['ts'][j] == et[i]):
                    sum += 1 / 60.0 * ds['f60600b'][j] * (1 + (ds['f606007'][j] - 40.0) / (40.0 - ds['f606006'][j]))
            ysl.append(sum)
            mac.append(aa)
            dd.append(mon + e)
        da['macid'] = mac
        da['dd'] = dd
        da['bt'] = bt
        da['et'] = et
        da['ysl'] = ysl
        for i in range(len(dh)):
            sum = 0
            for j in range(len(da)):
                if((j<len(da)) and ks[i]<js[i] and da['bt'][j]>=ks[i] and da['et'][j]<=js[i]):
                    sum += da['ysl'][j]
                if(ks[i] == js[i] and da['bt'][j] == ks[i]):
                    sum += da['ysl'][j]
            sl.append(sum)
        dh['sl'] = sl

        dz = pd.concat([dz, dh])
    dz = dz.drop_duplicates()
    dz.to_csv('u' + aa + '.csv', index=False)
    print da
    print dh
    endtime = datetime.now()
    print (endtime - starttime).seconds