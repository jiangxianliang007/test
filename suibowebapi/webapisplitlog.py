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
import taolelogs
from EventsDefine import EvensIDS
from EventsDefine import LoginType
from tabledefine import TableNameS
from EventsDefine import PayTypeName
session=None
kafka_hosts=[]



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
		taolelogs.logroot.warn(e)
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
		taolelogs.logroot.warn(e)
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
		taolelogs.logroot.warn(e)
		return False

#返回 {mode,source,uin,time}
# mode: 1 表示新注册用户，非0登录用户
# source: 表示登录来源,99表示手机
# uin: 用户
# time: 表示LOG发生的时间
# cid :表示来源的设备
def GetRegUinInfo(message):
	#得到注册时间
	if ('eventid' in message.value) and ('content' in message.value) and ('serTime' in message.value) and ('requestData' in message.value['content']) and ('clientId' in message.value['content']['requestData']):
		cid = message.value['content']['requestData']['clientId']
		if (cid == 'quokka_ios') or (cid == 'quokka_android'):
			if message.value['eventid'] == 10025:
				if 'retData' in message.value['content']:
					retjson=json.JSONDecoder().decode(message.value['content']['retData'])
					if ('lt_uin' in retjson) and ('first_authorization' in retjson) and ('source' in message.value['content']['requestData']) and ('clientChannel' in message.value['content']['requestData']):
						return {'mode':retjson['first_authorization'],'source':int(message.value['content']['requestData']['source']),'uin':int(retjson['lt_uin']),'time':message.value['serTime'],'cid':cid,\
								'channel':message.value['content']['requestData']['clientChannel']}
			elif message.value['eventid'] == 100154:
				if 'retData' in message.value['content'] and ('clientChannel' in message.value['content']['requestData']):
					retjson=json.JSONDecoder().decode(message.value['content']['retData'])
					if 'lt_uin' in retjson:
						return {'mode':2,'source':99,'uin':int(retjson['lt_uin']),'time':message.value['serTime'],'cid':cid,'channel':message.value['content']['requestData']['clientChannel']}
	return None

def SplitRegLoginSql(message):
	insertsql = None
	updatesql = None
	eventsql = None
	sqllist = {}
	ret = GetRegUinInfo(message)
	if ret == None:
		return sqllist
	if ret['mode'] >0:
		insertsql = "update suibo_user_info set regtime='%s',login_type=%d,clientchannel='%s' where uin = %d" % (ret['time'],ret['source'],ret['channel'],ret['uin'])
	else:
		insertsql = "update suibo_user_info set login_type=%d,clientchannel='%s' where uin = %d" % (ret['source'],ret['channel'],ret['uin'])

	if ret['mode'] >0:
		updatesql = "insert into suibo_user_info(regtime,uin,login_type,clientchannel) values('%s',%d,%d,'%s')" % (ret['time'],ret['uin'],ret['source'],ret['channel'])
	else:
		updatesql = "insert into suibo_user_info(uin,login_type,clientchannel) values(%d,%d,'%s')" % (ret['uin'],ret['source'],ret['channel'])
	commentstr = None
	if ret['mode'] >0:
		commentstr = u"第一次授权登陆,来源:%s 渠道:%s" % (LoginType.GetName(ret['source']),ret['channel'])
	else:
		commentstr = u"来源:%s 渠道:%s" % (LoginType.GetName(ret['source']),ret['channel'])
	eventsql=EvensIDS.GetEventSql(EvensIDS.EVENT_LOGIN_ID,ret['uin'],ret['time'],commentstr)
	sqllist['insert'] = insertsql
	sqllist['update'] = updatesql
	sqllist['event'] = eventsql
	return sqllist

#解析改名字
def splitChangNick(message):
	sqllist = {}
	insertsql = None
	eventstr = None
	if ('eventid' in message.value) and ('content' in message.value) and ('serTime' in message.value) and ('requestData' in message.value['content']) and ('clientId' in message.value['content']['requestData']) \
		and ('nick' in message.value['content']['requestData']) and ('uin' in message.value['content']['requestData']) and ('userIp' in message.value['content']):
		cid = message.value['content']['requestData']['clientId']
		if (cid == 'quokka_ios') or (cid == 'quokka_android'):
			if message.value['eventid'] == 100102:
				tablename= "suibo_user_action_" + time.strftime("%Y%m", time.localtime())
				insertsql = "insert into %s(action_time,uin,flag,ip) values('%s',%d,%d,'%s') ON DUPLICATE KEY UPDATE ip='%s'" %\
							(tablename,message.value['serTime'],int(message.value['content']['requestData']['uin']),6,message.value['content']['requestData']['nick'],message.value['content']['requestData']['nick'])
				commentstr = u"%s" % (message.value['content']['requestData']['nick'])
				eventstr = EvensIDS.GetEventSql(EvensIDS.EVENT_CHANGENAME_ID,message.value['content']['requestData']['uin'],message.value['serTime'],commentstr)
	sqllist['insert'] = insertsql
	sqllist['event'] = eventstr
	return sqllist


#解析購買
def splitWebBuy(message):
	sqllist = {}
	sqlstr = None
	eventsql = None
	eventstr = None
	if ('eventid' in message.value) and ('content' in message.value) and ('serTime' in message.value)  \
		and ('requestData' in message.value['content'])\
		and  ('out_trade_no' in message.value['content']['requestData']) \
		and ('body' in message.value['content']['requestData'])\
		and ('price' in message.value['content']['requestData']):
		if message.value['eventid']==10077:
			bodyjoson=json.JSONDecoder().decode(message.value['content']['requestData']['body'])
			if ('uin' in bodyjoson):
				uin = int(bodyjoson['uin'])
				date = message.value['serTime']
				date = date.replace('/','-')
				money = message.value['content']['retData']
				ordernum = message.value['content']['requestData']['out_trade_no']
				gid = bodyjoson['goodId']
				tablename = TableNameS.suibo_usr_buy + '_' + time.strftime("%Y%m", time.localtime())
				npaytype = 2
				sqlstr = "insert into %s(date,uin,cash,ordernum,gid,paytype)" \
			 		 " values('%s',%d,%s,'%s','%s',%d)"%(tablename,date,uin,money,ordernum, gid,npaytype)
				commentstr = u"充值:%s元,方式:%s,订单号:%s" % (money,PayTypeName.GetName(npaytype),ordernum)
				eventstr = EvensIDS.GetEventSql(EvensIDS.EVENT_BUY_ID,uin,date,commentstr)
	sqllist['insert'] = sqlstr
	sqllist['event'] = eventstr
	return sqllist

#解析購買
def splitAppleBuy(message):
	sqllist = {}
	sqlstr = None
	eventsql = None
	eventstr = None
	if ('eventid' in message.value) and ('content' in message.value) and ('serTime' in message.value)  \
		and ('requestData' in message.value['content']) and ('clientId' in message.value['content']['requestData']) and ('uin' in message.value['content']['requestData'])\
		and ('clientChannel' in message.value['content']['requestData']) \
		and ('clientVer' in message.value['content']['requestData']) and ('orderNum' in message.value['content']['requestData']) \
		and ('retData' in message.value['content']) and ('goods_id' in message.value['content']['requestData']):
		cid = message.value['content']['requestData']['clientId']
		if (message.value['eventid'] == 10029) and (cid == 'quokka_ios' or cid == 'quokka_android'):
			uin = int(message.value['content']['requestData']['uin'])
			date = message.value['serTime']
			date = date.replace('/','-')
			channel = message.value['content']['requestData']['clientChannel']
			clientver = message.value['content']['requestData']['clientVer']
			money = message.value['content']['retData']
			ordernum = message.value['content']['requestData']['orderNum']
			tablename = TableNameS.suibo_usr_buy + '_' + time.strftime("%Y%m", time.localtime())
			npaytype = 5
			sqlstr = "insert into %s(date,uin,cash,channel,clientver,ordernum,gid,paytype)" \
			 		 " values('%s',%d,%s,'%s','%s','%s','%s',%d)"%(tablename,date,uin,money,channel,clientver,ordernum,message.value['content']['requestData']['goods_id'],npaytype)
			commentstr = u"充值:%s元,方式:%s,订单号:%s" % (money,PayTypeName.GetName(npaytype),ordernum)
			eventstr = EvensIDS.GetEventSql(EvensIDS.EVENT_BUY_ID,uin,date,commentstr)
	sqllist['insert'] = sqlstr
	sqllist['event'] = eventstr
	return sqllist		
#解析紅包分享 還沒有加MAC功能
def splitHongBao(message):
	sqllist = {}
	sqlstr = None
	eventsql = None
	eventstr = None
	if ('eventid' in message.value) and ('content' in message.value) and ('serTime' in message.value)  \
		and ('requestData' in message.value['content']) and ('clientId' in message.value['content']['requestData']) and ('uin' in message.value['content']['requestData'])\
		and ('nickname' in message.value['content']['requestData']) and ('userIp' in message.value['content']) \
		and ('clientChannel' in message.value['content']['requestData']) and ('flag' in message.value['content']['requestData']) \
		and ('clientVer' in message.value['content']['requestData']) and ('share_url' in message.value['content']['requestData']):
		if (message.value['eventid'] == 100121) and ('hongbao/share.html' in message.value['content']['requestData']['share_url']):
			uin = int(message.value['content']['requestData']['uin'])
			date = message.value['serTime']
			date = date.replace('/','-')
			nick = message.value['content']['requestData']['nickname']
			ip = message.value['content']['userIp']
			flag = int(message.value['content']['requestData']['flag'])
			channel = message.value['content']['requestData']['clientChannel']
			clientver = message.value['content']['requestData']['clientVer']
			cid = message.value['content']['requestData']['clientId']
			sqlstr = "insert into hongbao_shareuin_info(tjdate,uin,nick,ip,flag,client_channel,client,client_ver)" \
			 		 " values('%s',%d,'%s','%s',%d,'%s','%s','%s')"%(date,uin,nick,ip,flag,channel,cid,clientver)
			commentstr = EvensIDS.GetEventName(EvensIDS.EVENT_HONGBAO_ID)
			eventstr = EvensIDS.GetEventSql(EvensIDS.EVENT_HONGBAO_ID,uin,date,commentstr)
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
	if eid == 100121 or eid == 100154 or eid == 10025 or eid == 100102 or eid == 10029 or \
	   eid == 10077:
		return True
	return False

def Split():
	global kafka_hosts
	consumer = KafkaConsumer('suibowebapilogs',
						 group_id='suibwebapi',
                         client_id="suibwebapi",
                         bootstrap_servers=kafka_hosts,value_deserializer=lambda m: json.loads(m.decode('utf-8')),auto_offset_reset="earliest", enable_auto_commit=True)
	for message in consumer:
		if not allowSplit(message):
			continue
		sqllist = SplitRegLoginSql(message)
		#print sqllist
		if sqllist.has_key('insert') and sqllist['insert']!= None:
			if savedbsqlalchemy(sqllist['insert']) == 0:
				if sqllist.has_key('update') and sqllist['update']!=None:
					savedbsqlalchemy(sqllist['update'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				#print sqllist['event']
				savedbsqlalchemy(sqllist['event'])
			continue

		sqllist = splitHongBao(message)
		if sqllist.has_key('insert') and sqllist['insert']!=None:
			savedbsqlalchemy(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				savedbsqlalchemy(sqllist['event'])
				#print sqllist['event']
			continue

		sqllist = splitChangNick(message)
		if sqllist.has_key('insert') and sqllist['insert']!=None:
			savedbsqlalchemy(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				#print sqllist['event']
				savedbsqlalchemy(sqllist['event'])
			continue

		sqllist = splitAppleBuy(message)
		if sqllist.has_key('insert') and sqllist['insert']!=None:
			savedbsqlalchemy(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				#print sqllist['event']
				savedbsqlalchemy(sqllist['event'])
			continue
		sqllist = splitWebBuy(message)
		if sqllist.has_key('insert') and sqllist['insert']!=None:
			savedbsqlalchemy(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				#print sqllist['event']
				savedbsqlalchemy(sqllist['event'])
			continue
	
def main():
	taolelogs.InitailLogs('suibowebapi')
	InitialDB()
	Split()
	

if __name__ == "__main__":
	main()
