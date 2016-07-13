#coding=utf-8
#!/usr/bin/ python
import string, os, sys
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
	sqlstr = None
	regstr = '(?P<date>\d+/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\|(?:\s+type:suibo\s+version:\d+.\d+.\d+.\d+\s+module:SEND_GIFT\s+eventid:769\s+pid:1\s+servid:\d+\s+roomid:)'\
		'(?P<roomid>\d+)(?:\s+src:)(?P<srcuin>\d+)(?:\s+vid:)(?P<vid>\d+_\d+)(?:\s+dst:)(?P<dstuin>\d+)(?:\s+presentMony:)(?P<sendmoney>\d+)(?:\s+combo:)(?P<combo>\d+)'
	pattern = re.compile(regstr)
	match = pattern.search(message.value['message'])
	if match:
		sqlstr = "insert into suibo_room_sendgift_info(tjdate,roomid,srcUin,vid,dstUin,money,combo) values('%s',%d,%d,'%s',%d,%d,%d)"%\
				(match.group('date'),int(match.group('roomid')),int(match.group('srcuin')),match.group('vid'),int(match.group('dstuin')),int(match.group('sendmoney')),int(match.group('combo')))
	return sqlstr
#解析聊天
def splitChat(message):
	sqlstr = None
	regstr = "(?P<date>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\| type:suibo version:(\d+.\d+.\d+.\d+) module:TEXT_CHAT eventid:768 pid:(\d+) servid:(?P<sid>\d+) roomid:(?P<roomid>\d+) anchorUin:(?P<anchoruin>\d+) vid:(?P<vid>\d+_\d+) srcUin:(?P<srcuin>\d+) dstUin:(?P<dstuin>\d+) msgtype:(\d+) is_public:(\d+) msg:(?P<text>.*)"
	pattern = re.compile(regstr)
	match = pattern.search(message.value['message'])
	if match:
		sqlstr = "insert into suibo_room_liaot_info(tjdate,roomid,anchorUin,vid,srcUin,msg) values('%s',%d,%d,'%s',%d,'%s')" % (match.group('date'),int(match.group('roomid')),int(match.group('anchoruin')),match.group('vid'),int(match.group('srcuin')),match.group('text'))
	return sqlstr

#解析登陸
def splitLL(message):
	sqlstr = None
	regstr = '(?P<date>\d+/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\|(?:\s+LL\s+)(?P<uin>\d+)(?:\s+logon room\s+)(?P<roomid>\d+)'\
				'(?:\s+devType\s+=\s+)(?P<devtype>\d+)(?:\s+)(?P<ip>\d+.\d+.\d+.\d+):(?:\d+\s+money\s+=\s+\d+,\s+recv\s+=\s+\d+'\
		 		'\s+accountState=\d+,\s+customFace=\d+,\s+rank\s+=\d+\s+\d+,\s+mac=)(?P<mac>[0-9a-zA-Z]{0,40})(?:\s+proxy)'\
		 		'(?P<proxyip>\d+.\d+.\d+.\d+):(?:\d+)(?:\s+enterpic=[0-9a-zA-Z._/]+\s+newpic=[0-9a-zA-Z._/]+\s+level\s+=\s+\d+\s+'\
		  		'ver=\d+\s+loginSpan\s+)(?P<span>\d+)'
	pattern = re.compile(regstr)
	match = pattern.search(message.value['message'])
	if match:
		sqlstr = "insert into suibo_room_login_info(tjdate,uin,roomid,devType,ip,mac,proxyip,loginSpan) values('%s',%d,%d,%d,'%s','%s','%s',%d)" %(match.group('date'),int(match.group('uin')),int(match.group('roomid')),int(match.group('devtype')),match.group('ip'),match.group('mac'),match.group('proxyip'),int(match.group('span')))
	return sqlstr

#解析總入口
def GetSqlStr(message):
	sqlstr = splitChat(message)
	if sqlstr!=None:
		print 'chat'
		return sqlstr
	sqlstr = splitSendGift(message)
	if sqlstr!=None:
		print 'sendgift'
		return sqlstr
	sqlstr = splitLL(message)
	if sqlstr!=None:
		print 'll'
		return sqlstr
	return None
		
def Split():
	global kafka_hosts
	consumer = KafkaConsumer('suiboltlog',
						 group_id='suibo',
                         client_id="suibochat",
                         bootstrap_servers=kafka_hosts,value_deserializer=lambda m: json.loads(m.decode('utf-8')),auto_offset_reset="earliest", enable_auto_commit=True)
	for message in consumer:
		sqlstr = None
		try:
			sqlstr = GetSqlStr(message)
		except Exception, e:
			root.warn(e)
		if sqlstr!=None:
			savedbsqlalchemy(sqlstr)
			print sqlstr


def main():
	InitialDB()
	Split()
	

if __name__ == "__main__":
	main()
