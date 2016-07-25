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
import taolelogs
from EventsDefine import EvensIDS
from EventsDefine import LoginType
from tabledefine import TableNameS
from EventsDefine import PayTypeName
from dbhelper import TaoleSessionDB
session=None
kafka_hosts=[]



def InitialDB():
	global kafka_hosts
	global session
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
		taolelogs.logroot.warn(e)
		exit(0)
	
	print "dbhost:%s dbport%s dbuser:%s dbpwd:%s broker_hosts:%s"%(db_host,db_port,db_user,db_pass,kafka_hosts)
	session = TaoleSessionDB(db_host,db_port,db_user,db_pass,'imsuibo')




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
	global session
	consumer = KafkaConsumer('sbowebapilogs',
						 group_id='sbowebapilogs',
                         client_id="sbowebapilogs",
                         bootstrap_servers=kafka_hosts,value_deserializer=lambda m: json.loads(m.decode('utf-8')),auto_offset_reset="earliest", enable_auto_commit=True)
	for message in consumer:
		if not allowSplit(message):
			continue
		sqllist = splitWebDrawMoney(message)
		if sqllist.has_key('insert') and sqllist['insert']!=None:
			session.excute(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				#print sqllist['event']
				session.excute(sqllist['event'])
			continue
def main():
	taolelogs.InitailLogs('sbowebapilogs')
	InitialDB()
	Split()
	

if __name__ == "__main__":
	main()
