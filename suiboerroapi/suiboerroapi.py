#coding=utf-8
#!/usr/bin/python
import string, os, sys
sys.path.append('../comm')
from kafka import KafkaConsumer
import ConfigParser
import json
import re
import datetime
import time
import threading
import taolelogs
from EventsDefine import EvensIDS
from EventsDefine import LoginType
from tabledefine import TableNameS
from EventsDefine import PayTypeName
from xml.etree import ElementTree as ET
from sendemail import sendEmail
from dbhelper import TaoleSessionDB
import Queue
session=None
kafka_hosts=[]
kafka_topic = ''
geidsdict= {}
emailque = Queue.Queue()

class  ErrItem(object):
	def __init__(self, id,titile,content):
		self.id = id
		self.titile = titile
		self.content = content
		self.exclude = []

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
		for eiditem in eids:
			eid = eiditem.attrib['id']
			emailtitle = eiditem.attrib['emailtitle']
			content = eiditem.attrib['content']
			item = ErrItem(eid,emailtitle,content)
			excls = eiditem.getchildren()
			for retcode in excls:
				item.exclude.append(retcode.text)
			geidsdict[eiditem.attrib['id']] = item
	except Exception, e:
		print Exception,e
		taolelogs.logroot.warn(e)
		exit(0)
	


def InitialDB():
	global kafka_hosts
	global kafka_topic
	cf = ConfigParser.ConfigParser()
	try:
		cf.read("db.conf")
		db_host = cf.get("db", "db_host")
		db_port = cf.getint("db", "db_port")
		db_user = cf.get("db", "db_user")
		db_pass = cf.get("db", "db_pass")
		kafka_hosts = cf.get("kafka","broker_hosts")
		#kafka_topic = cf.get("kafka",'topic')
	except Exception, e:
		print Exception,":",e
		taolelogs.logroot.warn(e)
		exit(0)
	
	print "dbhost:%s dbport%s dbuser:%s dbpwd:%s broker_hosts:%s"%(db_host,db_port,db_user,db_pass,kafka_hosts)
	global session
	session = TaoleSessionDB(db_host,db_port,db_user,db_pass,'imsuibo')




	
#能夠析的東西加在這裡
def allowSplit(message):
	eid = -1
	if ('eventid' in message.value):
		eid = message.value['eventid']
	else:
		return False
	if ( ('content' in message.value) and ('requestData' in message.value['content']) and \
		('clientId' in message.value['content']['requestData']) ):
		cid = message.value['content']['requestData']['clientId']
		if (cid != 'quokka_ios') and (cid != 'quokka_android'):
			return False
	if ('orderFrom' in message.value['content']):
		cid = message.value['content']['orderFrom']
		if (cid != 'suibo'):
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
		 	retcode = str(message.value['content']['retCode'])
		if  ('retMsg' in message.value['content']):
		 	retmsg = message.value['content']['retMsg']
		return {'eid':message.value['eventid'],'date':message.value['serTime'],'code':retcode,'errmsg':retmsg}
	return None

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
				content =  content + u'错误返回码:%s' % (ret['code'])
				if ret['code'] in geidsdict[str(message.value['eventid'])].exclude:
					continue
			if content=='':
				content = geidsdict[str(message.value['eventid'])].content
			content =  ret['date']+u' eventid:' + str(message.value['eventid']) + ' ' + content
			#print content
			#print  ret['date']
			#logging.log
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
	taolelogs.InitailLogs('suiboerrorapilogs')
	InitialDB()
	readErrxml()
	t=threading.Thread(target = SendEmail,args=())
	t.start()
	Split()

if __name__ == "__main__":
	main()
