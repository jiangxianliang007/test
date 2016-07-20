#coding=utf-8
#!/usr/bin/python

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
import datetime
import time
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
from EventsDefine import EvensIDS
from EventsDefine import LoginType
from tabledefine import TableNameS
from EventsDefine import PayTypeName
session=None
kafka_hosts=[]
if not os.path.exists('./logs/sbowebapilogs'):
	os.makedirs('./logs/sbowebapilogs')
level = logging.INFO  
format = '%(asctime)s %(levelname)s %(module)s.%(funcName)s Line:%(lineno)d %(message)s'  
hdlr = TimedRotatingFileHandler("./logs/sbowebapilogs/sbowebapilogs.log","D")  
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
		result = session.execute(sql)
		session.commit()
		return result.rowcount
	except Exception, e:
		print Exception,":",e
		root.warn(e)
		return False

def splitWebDrawMoney(message):
	sqllist = {}
	sqlstr = None
	eventsql = None
	eventstr = None
	if ('eventid' in message.value) and ('content' in message.value) and ('serTime' in message.value)  \
		and ('requestData' in message.value['content']) and ('uin' in message.value['content']['requestData']) \
		and ('money' in message.value['content']['requestData']):
		if message.value['eventid'] == 310000:
			date = message.value['serTime']
			money = message.value['content']['requestData']['money']
			uin = message.value['content']['requestData']['uin']
			tablename = TableNameS.suibo_drawmoney + '_' + time.strftime("%Y%m", time.localtime())
			sqlstr = "insert into %s(date,uin,money)" \
			 		 " values('%s',%d,%s)"%(tablename,date,int(uin),money)
			commentstr = u"申请提现:%s元" % (money)
			eventstr = EvensIDS.GetEventSql(EvensIDS.EVENT_DRAWMONEY_ID,uin,date,commentstr)
	sqllist['insert'] = sqlstr
	sqllist['event'] = eventstr
	return sqllist		
#能夠析的東西加在這裡
def allowSplit(message):
	eid = -1
	if ('eventid' in message.value):
		eid = int(message.value['eventid'])
	else:
		return False
	if eid == 310000:
		return True
	return False

def Split():
	global kafka_hosts
	consumer = KafkaConsumer('sbowebapilogs',
						 group_id='sbowebapilogs',
                         client_id="sbowebapilogs",
                         bootstrap_servers=kafka_hosts,value_deserializer=lambda m: json.loads(m.decode('utf-8')),auto_offset_reset="earliest", enable_auto_commit=True)
	for message in consumer:
		if not allowSplit(message):
			continue
		sqllist = splitWebDrawMoney(message)
		if sqllist.has_key('insert') and sqllist['insert']!=None:
			savedbsqlalchemy(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				#print sqllist['event']
				savedbsqlalchemy(sqllist['event'])
			continue
def main():
	InitialDB()
	Split()
	

if __name__ == "__main__":
	main()
