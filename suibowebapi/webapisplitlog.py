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
import datetime
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler

session=None
kafka_hosts=[]

if not os.path.exists('./logs/suibowebapi'):
	os.makedirs('./logs/suibowebapi')
level = logging.INFO  
format = '%(asctime)s %(levelname)s %(module)s.%(funcName)s Line:%(lineno)d %(message)s'  
hdlr = TimedRotatingFileHandler("./logs/suibowebapi/suibowebapi.log","D")  
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
					if ('lt_uin' in retjson) and ('first_authorization' in retjson) and ('source' in message.value['content']['requestData']):
						return {'mode':retjson['first_authorization'],'source':int(message.value['content']['requestData']['source']),'uin':int(retjson['lt_uin']),'time':message.value['serTime'],'cid':cid}
			elif message.value['eventid'] == 100154:
				if 'retData' in message.value['content']:
					retjson=json.JSONDecoder().decode(message.value['content']['retData'])
					if 'lt_uin' in retjson:
						return {'mode':2,'source':99,'uin':int(retjson['lt_uin']),'time':message.value['serTime'],'cid':cid}
	return None

def SplitRegLoginSql(message):
	insertsql = None
	updatesql = None
	sqllist = {}
	ret = GetRegUinInfo(message)
	if ret == None:
		return sqllist
	if ret['mode'] >0:
		insertsql = "update suibo_user_info set regtime='%s',login_type=%d where uin = %d" % (ret['time'],ret['source'],ret['uin'])
	else:
		insertsql = "update suibo_user_info set login_type=%d where uin = %d" % (ret['source'],ret['uin'])

	if ret['mode'] >0:
		updatesql = "insert into suibo_user_info(regtime,uin,login_type) values('%s',%d,%d)" % (ret['time'],ret['uin'],ret['source'])
	else:
		updatesql = "insert into suibo_user_info(uin,login_type) values(%d,%d)" % (ret['uin'],ret['source'])
	sqllist['insert'] = insertsql
	sqllist['update'] = updatesql
	return sqllist



#解析紅包分享 還沒有加MAC功能
def splitHongBao(message):
	sqllist = {}
	sqlstr = None
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

	sqllist['insert'] = sqlstr
	return sqllist		

#能夠析的東西加在這裡
def allowSplit(message):
	eid = -1
	if ('eventid' in message.value):
		eid = int(message.value['eventid'])
	else:
		return False
	if eid == 100121 or eid == 100154 or eid == 10025:
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
			continue
		
		sqllist = splitHongBao(message)
		if sqllist.has_key('insert') and sqllist['insert']!=None:
			savedbsqlalchemy(sqllist['insert'])
			continue
		
	
def main():
	InitialDB()
	Split()
	

if __name__ == "__main__":
	main()
