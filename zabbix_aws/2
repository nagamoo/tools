#!/bin/env python
# -*- coding:utf-8 -*-
#
# 複数アカウント、複数CFのCloudfrontの監視値を取得、送信
#
# Key直書きパターン注意
#
# 秒数、取得対象、集計方法を指定

import os
import boto3
from boto3.session import Session
#import datetime
from datetime import datetime, timedelta
import pytz
import pprint
import argparse
import logging


to_address  = 'hogehogehogehogehoge@gmail.com'


pp = pprint.PrettyPrinter(indent=4)

# Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)
sh.setFormatter(formatter)
fh = logging.FileHandler(filename= "/usr/lib/zabbix/externalscripts/log/cloudfront.py.log")
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(sh)
logger.addHandler(fh)
logger.info('#############################################')
logger.info('#############################################')
logger.info("KICK DATETIME: %s" % str(datetime.now()))


# 固定値
REGION     = "us-east-1"


# parser
parser = argparse.ArgumentParser(description='Cloudfront特化 3600秒 or 86400秒（1日）での \
各アカウント、各Distribution毎の指定サービスの値を合算して返す')


args = parser.parse_args()



# 対象起点日時 pointDate 計算
## 3600 1時間の場合
tzjst=pytz.timezone('Asia/Tokyo')





'''
response = client.get_metric_statistics(
        Namespace='AWS/CloudFront',
        MetricName='Requests',
        Dimensions=[
            {
                'Name': 'Region',
                'Value': 'Global'
            },
            {
                'Name': 'DistributionId',
                'Value': 'E1LCIO89SF1KME'
            },
        ],
        StartTime=datetime.datetime.utcnow() - datetime.timedelta(seconds=600),
        EndTime=datetime.datetime.utcnow(),
        Period=300,
        Statistics=['Sum']
)
pp.pprint(response)
'''

#####################################################################

def getAccountCFVal(client, distList, metric, pointDate, period):
    total = 0
    for dist in distList:
        total += getDistCFVal(client, dist, metric, pointDate, period)
    return total

def getDistCFVal(client, dist, metric, pointDate, period):
    logger.info("DIST %s" % dist)
    logger.info("METRIC %s" % metric)

    # 開始日時計算とUTCNOW変換
    startDate = pointDate - timedelta(seconds=period)
    startDate = startDate.astimezone(pytz.utc)
    endDate = pointDate.astimezone(pytz.utc)

    logger.info("startDate %s" % str(startDate))
    logger.info("endDate %s" % str(endDate))
    logger.info("period %s" % str(period))

    response = client.get_metric_statistics(
            Namespace='AWS/CloudFront',
            MetricName=metric,
            Dimensions=[
                {
                    'Name': 'Region',
                    'Value': 'Global'
                },
                {
                    'Name': 'DistributionId',
                    #'Value': 'E1LCIO89SF1KME'
                    'Value': dist
                },
            ],
            StartTime = startDate,
            EndTime = endDate,
            #Period=300,
            Period = int(period),
            Statistics=['Sum']
    )
    #pp.pprint(response)
    #logger.info(response)
    #pp.pprint(response['Datapoints'][0])
    #pp.pprint(response['Datapoints'][0]['Sum'])
    if len(response['Datapoints']) != 0:
        sumval = response['Datapoints'][0]['Sum']
        logger.info(response['Datapoints'][0]['Sum'] )
    else:
        sumval = 0
    logger.info("------------------------------")
    return sumval

def getCF1(bytesDL,request, pointDate, period):
    logger.info('#############################################')
    logger.info(" CF1 START ")

    accesskey = "XXXXXXXXXXX"
    secretkey = "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"

    distList=[]
    distList.append("XXXXXXXXXXXXXX")
    distList.append("YYYYYYYYYYYYYY")
    distList.append("ZZZZZZZZZZZZZZ")

    session = Session(
                      aws_access_key_id=accesskey,
                      aws_secret_access_key=secretkey,
                      region_name=REGION)
    client = session.client('cloudwatch')

    logger.info(" CF1 BytesDownloaded START ")
    bytesDL["CF1"] = getAccountCFVal(client, distList, 'BytesDownloaded', pointDate, period )
    logger.info(" CF1 Requests START ")
    request["CF1"] = getAccountCFVal(client, distList, 'Requests', pointDate, period )

def getCF4(bytesDL,request, pointDate, period):
    logger.info('#############################################')
    logger.info(" CF4 START ")

    accesskey = "XXXXXXXXXXX"
    secretkey = "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"

    distList=[]
    distList.append("XXXXXXXXXXXXXX")
    distList.append("YYYYYYYYYYYYYY")
    distList.append("ZZZZZZZZZZZZZZ")

    session = Session(
                      aws_access_key_id=accesskey,
                      aws_secret_access_key=secretkey,
                      region_name=REGION)
    client = session.client('cloudwatch')

    logger.info(" CF4 BytesDownloaded START ")
    bytesDL["CF4"] = getAccountCFVal(client, distList, 'BytesDownloaded', pointDate, period )
    logger.info(" CF3 Requests START ")
    request["CF4"] = getAccountCFVal(client, distList, 'Requests', pointDate, period )

def getCF3(bytesDL,request, pointDate, period):
    logger.info('#############################################')
    logger.info(" CF3 START ")
    # CF3
    accesskey = "XXXXXXXXXXX"
    secretkey = "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"

    distList=[]
    distList.append("XXXXXXXXXXXXXX")
    distList.append("YYYYYYYYYYYYYY")
    distList.append("ZZZZZZZZZZZZZZ")

    session = Session(
                      aws_access_key_id=accesskey,
                      aws_secret_access_key=secretkey,
                      region_name=REGION)
    client = session.client('cloudwatch')

    logger.info(" CF3 BytesDownloaded START ")
    bytesDL["CF3"] = getAccountCFVal(client, distList, 'BytesDownloaded', pointDate, period )
    logger.info(" CF3 Requests START ")
    request["CF3"] = getAccountCFVal(client, distList, 'Requests', pointDate, period )


def getCF2(bytesDL,request, pointDate, period):
    logger.info('#############################################')
    logger.info(" CF2 START ")

    accesskey = "XXXXXXXXXXX"
    secretkey = "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"

    distList=[]
    distList.append("XXXXXXXXXXXXXX")

    session = Session(
                      aws_access_key_id=accesskey,
                      aws_secret_access_key=secretkey,
                      region_name=REGION)
    client = session.client('cloudwatch')

    logger.info(" CF2 BytesDownloaded START ")
    bytesDL["CF2"] = getAccountCFVal(client, distList, 'BytesDownloaded', pointDate, period )
    logger.info(" CF2 Requests START ")
    request["CF2"] = getAccountCFVal(client, distList, 'Requests', pointDate, period )


def calcTotal(cfdict):
    # TOTAL 計算
    total = 0
    for key, cfval in cfdict.items():
        #logger.info("KEY %s" % key)
        #logger.info("VAL %s" % cfval)
        total += cfval
        #logger.info("TOTAL %s" % total)
    cfdict['TOTAL'] = total

def calcAvg(dl, req):
    if req != 0:
        return "{0:.3f} KB".format(dl / req / 1024)
    else:
        return "0 KB"

def setAvg(bytesDL, request):
    avg['CF4'] = calcAvg(bytesDL['CF4'], request['CF4'])
    avg['CF3'] = calcAvg(bytesDL['CF3'], request['CF3'])
    avg['CF1'] = calcAvg(bytesDL['CF1'] ,request['CF1'])
    avg['CF2'] = calcAvg(bytesDL['CF2'] ,request['CF2'])
    avg['TOTAL'] = calcAvg(bytesDL['TOTAL'], request['TOTAL'])
    return avg




def createMailBody(title, pointDate, period, bytesDL, request, avg):

    msg1  = u'\n'
    msg1 += u'--------------------------------------------\n'
    msg1 += u' [ %s ] \n' % title
    msg1 += u'\n'
    msg1 += u'StartDate : %s \n' % str(pointDate - timedelta(seconds=period))
    msg1 += u'  EndDate : %s \n' % str(pointDate)
    msg1 += u'   Period : %s \n' % str(period)
    msg1 += u'--------------------------------------------\n'
    msg1 += u'Average Object Size (KB): \n'
    msg1 += u"CF1     : {0} \n" .format(avg["CF1"])
    msg1 += u"CF2     : {0} \n" .format(avg["CF2"])
    msg1 += u"CF4     : {0} \n" .format(avg["CF4"])
    msg1 += u"CF3 : {0} \n" .format(avg["CF3"])
    msg1 += u'\n'
    msg1 += u"TOTAL    : {0} \n" .format(avg["TOTAL"])
    msg1 += u'\n'
    msg1 += u'--------------------------------------------\n'
    msg1 += u'BytesDownloaded(Byte): \n'
    msg1 += u"CF1     : {0:,d} \n" .format(int(bytesDL["CF1"]))
    msg1 += u"CF2     : {0:,d} \n" .format(int(bytesDL["CF2"]))
    msg1 += u"CF4     : {0:,d} \n" .format(int(bytesDL["CF4"]))
    msg1 += u"CF3 : {0:,d} \n" .format(int(bytesDL["CF3"]))
    msg1 += u'\n'
    msg1 += u"TOTAL    : {0:,d} \n" .format(int(bytesDL["TOTAL"]))
    msg1 += u"TOTAL(GB): {0:,.3f} \n" .format(int(bytesDL["TOTAL"])/1024/1024/1024)
    msg1 += u"TOTAL(TB): {0:,.3f} \n" .format(1.0 * int(bytesDL["TOTAL"])/1024/1024/1024/1024)
    msg1 += u"TOTAL(PB): {0:,.3f} \n" .format(1.0 * int(bytesDL["TOTAL"])/1024/1024/1024/1024/1024)
    msg1 += u'\n'
    msg1 += u'--------------------------------------------\n'
    msg1 += u'Requests: \n'
    msg1 += u"CF1     : {0:,d} \n" .format(int(request["CF1"]))
    msg1 += u"CF2     : {0:,d} \n" .format(int(request["CF2"]))
    msg1 += u"CF4     : {0:,d} \n" .format(int(request["CF4"]))
    msg1 += u"CF3 : {0:,d} \n" .format(int(request["CF3"]))
    msg1 += u'\n'
    msg1 += u"TOTAL    : {0:,d} \n" .format(int(request["TOTAL"]))
    msg1 += u'\n'
    return msg1

def createMailBodyHeader():

    msg1  = u'\n'
    msg1 += u'Cloudfront 平均Object Size \n'
    msg1 += u'\n'
    msg1 += u'実行日時 : %s \n' % str(datetime.now())
    msg1 += u'ZABBIX3 (192.168.16.192) \n'
    msg1 += u' %s \n' % os.path.abspath(__file__)
    msg1 += u'\n'
    msg1 += u'\n'
    return msg1

def sendmail(to_address, bodytext):
    import smtplib
    from email.MIMEText import MIMEText
    from email.Header import Header
    from email.Utils import formatdate

    from_address = 'zabbix@hogehoge.co.jp'

    charset = 'ISO-2022-JP'
    subject = u'AWS/Cloudfront Average Object Size {0}'.format(str(datetime.now().strftime("%Y/%m/%d %H:%M:%S")))
    text = createMailBodyHeader() + bodytext
    #text    = u'メールの本文です'


    msg = MIMEText(text.encode(charset), 'plain', charset)
    msg['Subject'] = Header(subject, charset)
    msg['From'] = from_address
    msg['To'] = to_address
    msg['Date'] = formatdate(localtime=True)

    smtp = smtplib.SMTP('localhost')
    smtp.sendmail(from_address, to_address, msg.as_string())
    smtp.close()

    # TEST
    return 0


def chatwork(bodytext):
    import requests

    BASE_URL = 'https://api.chatwork.com/v2'

    chatbody  = u'[info][title]AWS/Cloudfront Average Object Size {0}'.format(str(datetime.now().strftime("%Y/%m/%d %H:%M:%S"))) + "[/title]"
    chatbody += bodytext
    chatbody += '[/info]'

    #Setting
    roomid   = '0000000000' # AWSメンテナンス部屋

    message  = chatbody
    apikey   = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' #APIのKeyを記載

    post_message_url = '{}/rooms/{}/messages'.format(BASE_URL, roomid)

    headers = { 'X-ChatWorkToken': apikey}
    params = { 'body': message }
    r = requests.post(post_message_url,
                        headers=headers,
                        params=params)
    print(r)


####################################################################################################
####################################################################################################

if __name__ == '__main__':

    # 月初から現在まで
    # 1日の場合は前月？
    # periodは計算
    bytesDL = {'CF1':0, 'CF2':0, 'CF4':0, 'CF3':0, 'TOTAL':0}    # account=>ValueのDict
    request = {'CF1':0, 'CF2':0, 'CF4':0, 'CF3':0, 'TOTAL':0}    # 同様
    avg     = {'CF1':0, 'CF2':0, 'CF4':0, 'CF3':0, 'TOTAL':0}    #
    now = datetime.now() # EndDate は当日0時
    #now = datetime(2017, 10, 1, 0, 0, 0, tzinfo=tzjst)

    if now.day != 1:
        pointDate = datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=tzjst)
        startDate = datetime(now.year, now.month, 1, 0, 0, 0,  tzinfo=tzjst)
        delta = pointDate - startDate
        period = delta.total_seconds()
        #pointDate = timezone('UTC').localize(pointDate)

    else :
        # 月初の場合
        # PointDateは前月末日23:59:59＝実行日 00:00:00から-1Sec
        # period は PointDate-前月月初
        #pointDate = datetime(now.year, now.month, 1, 0, 0, 0, tzinfo=tzjst) - timedelta(seconds=1)
        lastMonthDate = datetime(now.year, now.month, 1, 0, 0, 0, tzinfo=tzjst) - timedelta(seconds=1)
        startDate = datetime(lastMonthDate.year, lastMonthDate.month, 1, 0, 0, 0, tzinfo=tzjst)
        pointDate = datetime(now.year, now.month, 1, 0, 0, 0, tzinfo=tzjst)
        delta = pointDate - startDate
        period = delta.total_seconds()
    logger.info("StartDate %s" % str(pointDate - timedelta(seconds=period)))
    logger.info("EndDate %s" % str(pointDate))

    getCF1(bytesDL,request, pointDate, period)
    getCF2(bytesDL,request, pointDate, period)
    getCF4(bytesDL,request, pointDate, period)
    getCF3(bytesDL,request , pointDate, period)
    logger.info('#############################################')

    calcTotal(bytesDL)
    calcTotal(request)

    avg = setAvg(bytesDL, request)

    mailbody = createMailBody(u"月初から昨日まで", pointDate, period, bytesDL, request, avg)
    mailbody += u'############################################\n'
    #logger.info(avg)
    #logger.info(bytesDL)
    #logger.info(request)



    # 前日分
    bytesDL = {'CF1':0, 'CF2':0, 'CF4':0, 'CF3':0, 'TOTAL':0}    # account=>ValueのDict
    request = {'CF1':0, 'CF2':0, 'CF4':0, 'CF3':0, 'TOTAL':0}    # 同様
    avg     = {'CF1':0, 'CF2':0, 'CF4':0, 'CF3':0, 'TOTAL':0}    #

    period = 86400
    now = datetime.now() # PointDate= EndDateは実行日0時
    pointDate = datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=tzjst)
    logger.info("StartDate %s" % str(pointDate - timedelta(seconds=period)))
    logger.info("EndDate %s" % str(pointDate))

    getCF1(bytesDL,request, pointDate, period)
    getCF2(bytesDL,request, pointDate, period)
    getCF4(bytesDL,request, pointDate, period)
    getCF3(bytesDL,request , pointDate, period)
    logger.info('#############################################')

    calcTotal(bytesDL)
    calcTotal(request)

    avg = setAvg(bytesDL, request)

    mailbody += createMailBody(u"前日分", pointDate, period, bytesDL, request, avg)
    mailbody += u'\n--------------------------------------------\n'
    mailbody += u'send by zabbix 192.168.16.192\n--------------------------------------------'

    logger.info(mailbody)
    #logger.info(avg)
    #logger.info(bytesDL)
    #logger.info(request)


    chatwork(mailbody)

    sendmail(to_address, mailbody)
