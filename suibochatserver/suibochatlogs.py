#coding=utf-8
#!/usr/bin/ python
import string, os, sys
sys.path.append('../comm')
from kafka import KafkaConsumer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import ConfigParser
import json
import re
import MySQLdb
import sqlalchemy
from sqlalchemy.pool import NullPool
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
from EventsDefine import EvensIDS
from EventsDefine import LoginType
import time
session=None
kafka_hosts=[]
if not os.path.exists('./logs/suibochat'):
	os.makedirs('./logs/suibochat')
level = logging.INFO  
format = '%(asctime)s %(levelname)s %(module)s.%(funcName)s Line:%(lineno)d %(message)s'  
hdlr = TimedRotatingFileHandler("./logs/suibochat/suibochat.log","D")  
fmt = logging.Formatter(format)  
hdlr.setFormatter(fmt)  
root = logging.getLogger()
root.addHandler(hdlr)  
root.setLevel(level)



def InitialDB():
	global kafka_hosts
	cf = ConfigParser.ConfigParser()
	try:
		cf.read("db.conf")
		db_host = cf.get("db", "db_host")
		db_port = cf.getint("db", "db_port")
		db_user = cf.get("db", "db_user")
		db_pass = cf.get("db", "db_pass")
		kafka_hosts = cf.get("kafka","broker_hosts")
	except Exception, e:
		print Exception,":",e
		root.warn(e)
		exit(0)
	
	print "dbhost:%s dbport%s dbuser:%s dbpwd:%s broker_hosts:%s"%(db_host,db_port,db_user,db_pass,kafka_hosts)
	global session
	DB_CONNECT_STRING = "mysql+mysqldb://%s:%s@%s:%s/imsuibo?charset=utf8" % (db_user,db_pass,db_host,db_port) 
	engine = create_engine(DB_CONNECT_STRING, echo=False)
	DB_Session = sessionmaker(bind=engine)
	session = DB_Session()
	try:
		session.execute("SET NAMES 'utf8mb4'")
	except Exception, e:
		print Exception,":",e
		root.warn(e)
		exit(0)

def  CloseDB():
	global session
	session.close()




def savedbsqlalchemy(sql):
	global session
	try:
		session.execute(sql)
		session.commit()
	except Exception, e:
		print Exception,":",e
		root.warn(e)
#解析刷花
def splitSendGift(message):
	insertsql = None
	eventsql = None
	sqllist = {}
	regstr = '(?P<date>\d+/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\|(?:\s+type:suibo\s+version:\d+.\d+.\d+.\d+\s+module:SEND_GIFT\s+eventid:769\s+pid:1\s+servid:\d+\s+roomid:)'\
		'(?P<roomid>\d+)(?:\s+src:)(?P<srcuin>\d+)(?:\s+vid:)(?P<vid>\d+_\d+)(?:\s+dst:)(?P<dstuin>\d+)(?:\s+presentMony:)(?P<sendmoney>\d+)(?:\s+combo:)(?P<combo>\d+)'
	pattern = re.compile(regstr)
	match = pattern.search(message.value['message'])
	if match:
		insertsql = "insert into suibo_room_sendgift_info(tjdate,roomid,srcUin,vid,dstUin,money,combo) values('%s',%d,%d,'%s',%d,%d,%d)"%\
				(match.group('date'),int(match.group('roomid')),int(match.group('srcuin')),match.group('vid'),int(match.group('dstuin')),int(match.group('sendmoney')),int(match.group('combo')))
		commentstr = u"%s在主播号%s送了%d乐豆,连击%d次"%(EvensIDS.GetEventName(EvensIDS.EVENT_SENDGIFT_ID),\
					match.group('vid'),int(match.group('sendmoney')),int(match.group('combo')))
		eventsql = EvensIDS.GetEventSql(EvensIDS.EVENT_SENDGIFT_ID,int(match.group('srcuin')),match.group('date'),commentstr)
	sqllist['insert'] = insertsql
	sqllist['event'] = eventsql
	return sqllist
#解析聊天
def splitChat(message):
	insertsql = None
	sqllist = {}
	regstr = "(?P<date>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\| type:suibo version:(\d+.\d+.\d+.\d+) module:TEXT_CHAT eventid:768 pid:(\d+) servid:(?P<sid>\d+) roomid:(?P<roomid>\d+) anchorUin:(?P<anchoruin>\d+) vid:(?P<vid>\d+_\d+) srcUin:(?P<srcuin>\d+) dstUin:(?P<dstuin>\d+) msgtype:(\d+) is_public:(\d+) msg:(?P<text>.*)"
	pattern = re.compile(regstr)
	match = pattern.search(message.value['message'])
	if match:
		insertsql = "insert into suibo_room_liaot_info(tjdate,roomid,anchorUin,vid,srcUin,msg) values('%s',%d,%d,'%s',%d,'%s')" % (match.group('date'),int(match.group('roomid')),int(match.group('anchoruin')),match.group('vid'),int(match.group('srcuin')),match.group('text'))
	sqllist['insert'] = insertsql
	return sqllist



#解析登陸
def splitLL(message):
	insertsql = None
	eventsql = None
	sqllist = {}
	regstr = '(?P<date>\d+/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\|(?:\s+LL\s+)(?P<uin>\d+)(?:\s+logon room\s+)(?P<roomid>\d+)'\
				'(?:\s+devType\s+=\s+)(?P<devtype>\d+)(?:\s+)(?P<ip>\d+.\d+.\d+.\d+):(?:\d+\s+money\s+=\s+\d+,\s+recv\s+=\s+\d+'\
		 		'\s+accountState=\d+,\s+customFace=\d+,\s+rank\s+=\d+\s+\d+,\s+mac=)(?P<mac>[0-9a-zA-Z]{0,40})(?:\s+proxy)'\
		 		'(?P<proxyip>\d+.\d+.\d+.\d+):(?:\d+)(?:\s+enterpic=[0-9a-zA-Z._/]+\s+newpic=[0-9a-zA-Z._/]+\s+level\s+=\s+\d+\s+'\
		  		'ver=\d+\s+loginSpan\s+)(?P<span>\d+)'
	pattern = re.compile(regstr)
	match = pattern.search(message.value['message'])
	if match:
		insertsql = "insert into suibo_room_login_info(tjdate,uin,roomid,devType,ip,mac,proxyip,loginSpan) values('%s',%d,%d,%d,'%s','%s','%s',%d)" %(match.group('date'),int(match.group('uin')),int(match.group('roomid')),int(match.group('devtype')),match.group('ip'),match.group('mac'),match.group('proxyip'),int(match.group('span')))
		commentstr = u"%s 房间号:%d" % (EvensIDS.GetEventName(EvensIDS.EVENT_LOGINCHAT_ID),int(match.group('roomid')))
		eventsql = EvensIDS.GetEventSql(EvensIDS.EVENT_LOGINCHAT_ID,int(match.group('uin')),match.group('date'),commentstr)
	sqllist['insert'] = insertsql
	sqllist['event'] = eventsql
	return sqllist

#解析出房间
def splitLO(message):
	insertsql = None
	eventsql = None
	sqllist = {}
	regstr = '(?P<date>\d+/\d+/\d+\s+\d+:\d+:\d+)\|(?:\s+type:suibo\s+version:\d+.\d+.\d+.\d+\s+module:LOG_OUT_ROOM\s+eventid:257\s+pid:1\s+servid:\d+\s+roomid:)'\
			'(?P<roomid>\d+)(?:\s+uin:)(?P<uin>\d+)(?:\s+ver:\d+\s+ip:\d+.\d+.\d+.\d+\s+port:\d+\s+proxyIp:\d+.\d+.\d+.\d+\s+proxyPort:\d+\s+mac:[0-9a-zA-Z]{0,40})'\
			'(?:\s+startTime:\d+-\d+-\d+\s+\d+:\d+:\d+\s+endTime:\d+-\d+-\d+\s+\d+:\d+:\d+\s+keepTime:)(?P<time>\d+)'
	pattern =re.compile(regstr)
	match = pattern.search(message.value['message'])
	if match:
		tablename= "suibo_usr_logout_" + time.strftime("%Y%m", time.localtime())
		insertsql = "insert into %s(date,uin,roomid,vid,time) values('%s',%d,%d,'%s',%d)"%\
					(tablename,match.group('date'),int(match.group('uin')),int(match.group('roomid')),"",int(match.group('time')))
		commentstr = u"%s房间号:%d,观看时长:%d小时%d分%d秒" % (EvensIDS.GetEventName(EvensIDS.EVENT_LOGOUT_ID),int(match.group('roomid')),int(match.group('time'))/3600,int(match.group('time'))/60,int(match.group('time'))%60)
		eventsql = EvensIDS.GetEventSql(EvensIDS.EVENT_LOGOUT_ID,int(match.group('uin')),match.group('date'),commentstr)
	sqllist['insert'] = insertsql
	sqllist['event'] = eventsql
	return 	sqllist

def  splitTerminateVideo(message):
	insertsql = None
	eventsql = None
	sqllist = {}
	regstr = '(?P<date>\d+/\d+/\d+\s+\d+:\d+:\d+)\|(?:\s+roomid=)(?P<roomid>\d+)(?:\s+update\s+user\s+vid\s+)(?P<vid>\d+_\d+)(?:\s+viewNum=)(?P<viewnum>\d+)(?:\s+viewTime=)(?P<viewtime>\d+)(?:\s+duration\s+)(?P<duration>\d+)'\
			'(?:\s+laudCount\s+)(?P<laudcount>\d+)(?:\s+vState\s+)(?P<vstate>\d+)(?:\s+webCurrNum=\d+\s+currNum=)(?P<currnum>\d+)'
	pattern =re.compile(regstr)
	match = pattern.search(message.value['message'])
	if match:
		if (int(match.group('vstate')) == 1) or (int(match.group('vstate'))==5):
			tablename= "suibo_usr_closevideo_" + time.strftime("%Y%m", time.localtime())
			insertsql = "insert into %s(date,vid,viewnum,laudcount) values('%s','%s',%d,%d)"%\
					(tablename,match.group('date'),match.group('vid'),int(match.group('viewnum')),int(match.group('laudcount')))
			commentstr = u"%svid:%s,观看人数:%d,点赞总数:%d" % (EvensIDS.GetEventName(EvensIDS.EVENT_TEMINATEVIDEO_ID),match.group('vid'),int(match.group('viewnum')),int(match.group('laudcount')))
			vidtemplst = match.group('vid').split('_')
			eventsql = EvensIDS.GetEventSql(EvensIDS.EVENT_TEMINATEVIDEO_ID,int(vidtemplst[0]),match.group('date'),commentstr)
	sqllist['insert'] = insertsql
	sqllist['event'] = eventsql
	return 	sqllist

def Split():
	global kafka_hosts
	consumer = KafkaConsumer('suiboltlog',
						 group_id='suibo',
                         client_id="suibochat",
                         bootstrap_servers=kafka_hosts,value_deserializer=lambda m: json.loads(m.decode('utf-8')),auto_offset_reset="earliest", enable_auto_commit=True)
	for message in consumer:
		sqllist = splitChat(message)
		if sqllist.has_key('insert') and sqllist['insert']!= None:
			savedbsqlalchemy(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				savedbsqlalchemy(sqllist['event'])
			continue
		sqllist = splitSendGift(message)
		if sqllist.has_key('insert') and sqllist['insert']!= None:
			savedbsqlalchemy(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				savedbsqlalchemy(sqllist['event'])
			continue
		sqllist = splitLL(message)
		if sqllist.has_key('insert') and sqllist['insert']!= None:
			savedbsqlalchemy(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				savedbsqlalchemy(sqllist['event'])
			continue

		sqllist = splitLO(message)
		if sqllist.has_key('insert') and sqllist['insert']!= None:
			savedbsqlalchemy(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				savedbsqlalchemy(sqllist['event'])
			continue

		sqllist = splitTerminateVideo(message)
		if sqllist.has_key('insert') and sqllist['insert']!= None:
			savedbsqlalchemy(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				savedbsqlalchemy(sqllist['event'])
			continue


def main():
	InitialDB()
	Split()
	

if __name__ == "__main__":
	main()
