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
from EventsDefine import EvensIDS
from EventsDefine import LoginType
from tabledefine import TableNameS
from EventsDefine import PayTypeName
import taolelogs
from dbhelper import TaoleSessionDB

session=None
kafka_hosts=[]
kafka_topic = ''


def InitialDB():
	global kafka_hosts
	global session
	global kafka_topic
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




def splitWebBuy4100(message):
	sqllist = {}
	sqlstr = None
	eventsql = None
	eventstr = None
	if ('eventid' in message.value) and ('content' in message.value) and ('serTime' in message.value)  \
		and ('uin' in message.value) and ('orderInfo' in message.value['content']) and ('orderFrom' in message.value['content']) \
		and ('ordertype' in message.value['content']):
		if int(message.value['content']['ordertype'])!=1:
			return sqllist
		orderjson = json.JSONDecoder().decode(message.value['content']['orderInfo'])
		uin = int(message.value['uin'])
		date = message.value['serTime']
		date = date.replace('/','-')
		money = 0
		npaytype = ''
		paytype = 0
		cid = message.value['content']['orderFrom']
		if cid == 'suibo':
			if ('orderMoney' in message.value['content']):
				money = message.value['content']['orderMoney']
			if ('orderNum' in orderjson):
				ordernum = orderjson['orderNum']
			if ('pay_type' in orderjson):
				npaytype = int(orderjson['pay_type'])
			if message.value['eventid'] == 410000:
				tablename = TableNameS.suibo_usr_buy + '_' + time.strftime("%Y%m", time.localtime())
				sqlstr = "insert into %s(date,uin,cash,ordernum,gid,paytype)" \
			 		 " values('%s',%d,%s,'%s','%s',%d)"%(tablename,date,uin,money,ordernum,orderjson['goods_id'],npaytype)
				commentstr = u"充值:%s元,方式:%s,订单号:%s" % (money,PayTypeName.GetName(npaytype),ordernum)
				eventstr = EvensIDS.GetEventSql(EvensIDS.EVENT_BUY_ID,uin,date,commentstr)
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
	if eid ==410000:
		return True
	return False

def Split():
	global kafka_hosts
	global session
	consumer = KafkaConsumer('tlewebapilogs',
						 group_id='tlewebapilogs',
                         client_id="tlewebapilogs",
                         bootstrap_servers=kafka_hosts,value_deserializer=lambda m: json.loads(m.decode('utf-8')),auto_offset_reset="earliest", enable_auto_commit=True)
	for message in consumer:
		if not allowSplit(message):
			continue
		sqllist = splitWebBuy4100(message)
		if sqllist!=None and sqllist.has_key('insert') and sqllist['insert']!=None:
			session.excute(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				#print sqllist['event']
				session.excute(sqllist['event'])
			continue
	
def main():
	taolelogs.InitailLogs('tlwebapilogs')
	InitialDB()
	Split()
	

if __name__ == "__main__":
	main()
