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
import threading
from logging.handlers import TimedRotatingFileHandler
from EventsDefine import EvensIDS
from EventsDefine import LoginType
from tabledefine import TableNameS
from EventsDefine import PayTypeName
from xml.etree import ElementTree as ET
from sendemail import sendEmail
import Queue
session=None
kafka_hosts=[]
geidsdict= {}
emailque = Queue.Queue()

if not os.path.exists('./logs/suiboerrorapilogs'):
	os.makedirs('./logs/suiboerrorapilogs')
level = logging.INFO  
format = '%(asctime)s %(levelname)s %(module)s.%(funcName)s Line:%(lineno)d %(message)s'  
hdlr = TimedRotatingFileHandler("./logs/suiboerrorapilogs/suiboerrorapilogs.log","D")  
fmt = logging.Formatter(format)  
hdlr.setFormatter(fmt)  
root = logging.getLogger()
root.addHandler(hdlr)  
root.setLevel(level)
class  ErrItem(object):
	def __init__(self, id,titile,content):
		self.id = id
		self.titile = titile
		self.content = content

class alertQueItem(object):
	"""docstring for alertQueItem"""
	def __init__(self, date,eid,errmsg,emailtitle):
		super(alertQueItem, self).__init__()
		self.date = date
		self.eid = eid
		self.errmsg = errmsg
		self.emailtitle = emailtitle
		
def  readErrxml():
	geidsdict.clear()
	try:
		tree=ET.parse('errorsids.xml')
		root = tree.getroot()
		eids = root.getchildren()
		for eid in eids:
			item = ErrItem(eid.attrib['id'],eid.attrib['emailtitle'],eid.attrib['content'])
			geidsdict[eid.attrib['id']] = item
	except Exception, e:
		print Exception,e
	


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

	
#能夠析的東西加在這裡
def allowSplit(message):
	eid = -1
	if ('eventid' in message.value):
		eid = message.value['eventid']
	else:
		return False
	if str(eid) in geidsdict:
		return True
	return False

def splitAlert(message):
	retcode = ""
	retmsg = ""
	date = ""
	if ('eventid' in message.value) and ('content' in message.value) and ('serTime' in message.value):
		if ('retCode' in message.value['content']):
		 	retcode =message.value['content']['retCode']
		if  ('retMsg' in message.value['content']):
		 	retmsg = message.value['content']['retMsg']
	return {'eid':message.value['eventid'],'date':message.value['serTime'],'code':retcode,'errmsg':retmsg}

def Split():
	global kafka_hosts
	consumer = KafkaConsumer('suiboerrorapilogs',
						 group_id='suiboerrorapilogs',
                         client_id="suiboerrorapilogs",
                         bootstrap_servers=kafka_hosts,value_deserializer=lambda m: json.loads(m.decode('utf-8')),auto_offset_reset="earliest", enable_auto_commit=True)
	for message in consumer:
		if not allowSplit(message):
			continue
		ret = splitAlert(message)
		if ret!=None and (str(message.value['eventid']) in geidsdict):
			emailtitle = geidsdict[str(message.value['eventid'])]
			subject = geidsdict[str(message.value['eventid'])].titile
			content = ''
			if ('errmsg' in ret) and ret['errmsg']!='':
				content =  u'错误消息:' + ret['errmsg'] + ' '
			if ('code' in ret):
				content =  content + u'错误返回码:' + str(ret['code'])
			if content=='':
				content = geidsdict[str(message.value['eventid'])].content
			content =  ret['date']+u' eventid:' + str(message.value['eventid']) + ' ' + content
			alertitem = alertQueItem(ret['date'],message.value['eventid'],content,subject)
			emailque.put(alertitem)
	
def SendEmail():
	while True:
		item = emailque.get()
		tousrs = ['zhw@taolesoft.com','lqq@taolesoft.com','jxl@taolesoft.com',\
				'pxg@taolesoft.com','whg@taolesoft.com','djb@taolesoft.com',\
				'nyh@taolesoft.com','ql@taolesoft.com','chc@taolesoft.com']
		sendEmail(item.emailtitle,tousrs,'web API errors',item.errmsg)

def main():
	InitialDB()
	readErrxml()
	t=threading.Thread(target = SendEmail,args=())
	t.start()
	Split()

if __name__ == "__main__":
	main()
