#coding=utf-8
#!/usr/bin/python
import string, os, sys
sys.path.append('../comm')
from kafka import KafkaConsumer
import ConfigParser
import json
import re
import taolelogs
from EventsDefine import EvensIDS
from EventsDefine import LoginType
from tools import SuiboGetIP
from tools import GetTimeStr
from tabledefine import TableNameS
from dbhelper import TaoleSessionDB
import time
session=None
imquokkaDBsession = None
kafka_hosts=[]


def InitialimquokkaDB():
	cf = ConfigParser.ConfigParser()
	global imquokkaDBsession
	try:
		cf.read("db.conf")
		db_host = cf.get("db", "imquokkadb_host")
		db_port = cf.getint("db", "imquokkadb_port")
		db_user = cf.get("db", "imquokkadb_user")
		db_pass = cf.get("db", "imquokkadb_pass")
	except Exception, e:
		print Exception,e
		taolelogs.logroot.warn(e)
		exit(0)
	imquokkaDBsession = TaoleSessionDB(db_host,db_port,db_user,db_pass,'imquokka')
	

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
	session = TaoleSessionDB(db_host,db_port,db_user,db_pass,'imsuibo')
	try:
		session.excute("SET NAMES 'utf8mb4'")
	except Exception, e:
		print Exception,":",e
		taolelogs.logroot.warn(e)
		exit(0)




#解析刷花
def splitSendGift(message):
	insertsql = None
	eventsql = None
	eventsql2 = None
	sqllist = {}
	regstr = '(?P<date>\d+/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\|(?:\s+type:suibo\s+version:\d+.\d+.\d+.\d+\s+module:SEND_GIFT\s+eventid:769\s+pid:1\s+servid:\d+\s+roomid:)'\
		'(?P<roomid>\d+)(?:\s+src:)(?P<srcuin>\d+)(?:\s+vid:)(?P<vid>\d+_\d+)(?:\s+dst:)(?P<dstuin>\d+)(?:\s+presentMony:)(?P<sendmoney>\d+)(?:\s+combo:)(?P<combo>\d+)'
	pattern = re.compile(regstr)
	match = pattern.search(message.value['message'])
	if match:
		tablename = TableNameS.suibo_room_sendgift_info# + '_' + time.strftime("%Y%m", time.localtime())
		insertsql = "insert into %s(tjdate,roomid,srcUin,vid,dstUin,money,combo) values('%s',%d,%d,'%s',%d,%d,%d)"%\
				(tablename,match.group('date'),int(match.group('roomid')),int(match.group('srcuin')),match.group('vid'),int(match.group('dstuin')),int(match.group('sendmoney')),int(match.group('combo')))
		commentstr = u"在主播号%s送了%d乐豆(%f人民币),连击%d次"%(match.group('vid'),int(match.group('sendmoney')),int(match.group('sendmoney'))/float(100000),int(match.group('combo')))
		eventsql = EvensIDS.GetEventSql(EvensIDS.EVENT_SENDGIFT_ID,int(match.group('srcuin')),match.group('date'),commentstr)
		commentstr2 =  u"在主播号%s收了用戶%d送的%d乐豆(%f人民币) 连击%d次"%(match.group('vid'),int(match.group('srcuin')),int(match.group('sendmoney')),int(match.group('sendmoney'))/float(100000),int(match.group('combo')))
		eventsql2 = EvensIDS.GetEventSql(EvensIDS.EVENT_RECVGIFT_ID,int(match.group('dstuin')),match.group('date'),commentstr2)
	sqllist['insert'] = insertsql
	sqllist['event'] = eventsql
	sqllist['event2'] = eventsql2
	return sqllist
#解析聊天
def splitChat(message):
	insertsql = None
	sqllist = {}
	regstr = "(?P<date>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\| type:suibo version:(\d+.\d+.\d+.\d+) module:TEXT_CHAT eventid:768 pid:(\d+) servid:(?P<sid>\d+) roomid:(?P<roomid>\d+) anchorUin:(?P<anchoruin>\d+) vid:(?P<vid>\d+_\d+) srcUin:(?P<srcuin>\d+) dstUin:(?P<dstuin>\d+) msgtype:(\d+) is_public:(\d+) msg:(?P<text>.*)"
	pattern = re.compile(regstr)
	match = pattern.search(message.value['message'])
	if match:
		tablename = TableNameS.suibo_room_liaot_info #+ '_' + time.strftime("%Y%m", time.localtime())
		insertsql = "insert into %s(tjdate,roomid,anchorUin,vid,srcUin,msg) values('%s',%d,%d,'%s',%d,'%s')" % (tablename,match.group('date'),int(match.group('roomid')),int(match.group('anchoruin')),match.group('vid'),int(match.group('srcuin')),match.group('text'))
	sqllist['insert'] = insertsql
	return sqllist



#解析登陸
def splitLL(message):
	insertsql = None
	eventsql = None
	sqllist = {}
	regstr = '(?P<date>\d+/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\|(?:\s+LL\s+)(?P<uin>\d+)(?:\s+logon room\s+)(?P<roomid>\d+)'\
				'(?:\s+devType\s+=\s+)(?P<devtype>\d+)(?:\s+)(?P<ip>\d+.\d+.\d+.\d+):(?:\d+\s+money\s+=\s+\d+,\s+recv\s+=\s+\d+'\
		 		'\s+accountState=\d+,\s+customFace=\d+,\s+rank\s+=\d+\s+\d+,\s+mac=)(?P<mac>[0-9a-zA-Z]{0,60})(?:\s+proxy)'\
		 		'(?P<proxyip>\d+.\d+.\d+.\d+):(?:\d+)(?:\s+enterpic=[0-9a-zA-Z._/]*\s+newpic=[0-9a-zA-Z._/]*\s+level\s+=\s+\d+\s+'\
		  		'ver=\d+\s+loginSpan\s+)(?P<span>\d+)'
	pattern = re.compile(regstr)
	match = pattern.search(message.value['message'])
	if match:
		tablename = TableNameS.suibo_room_login_info# + '_' + time.strftime("%Y%m", time.localtime())
		insertsql = "insert into %s(tjdate,uin,roomid,devType,ip,mac,proxyip,loginSpan) values('%s',%d,%d,%d,'%s','%s','%s',%d)" %(tablename,match.group('date'),int(match.group('uin')),int(match.group('roomid')),int(match.group('devtype')),match.group('ip'),match.group('mac'),match.group('proxyip'),int(match.group('span')))
		ipaddr = SuiboGetIP(match.group('ip'))
		if ipaddr==None:
			ipaddr = u'未知地址'
		commentstr = u"房间号:%d,登陆IP:%s %s" % (int(match.group('roomid')),match.group('ip'),ipaddr)
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
		tablename= TableNameS.suibo_usr_logout + '_' + time.strftime("%Y%m", time.localtime())
		insertsql = "insert into %s(date,uin,roomid,vid,time) values('%s',%d,%d,'%s',%d)"%\
					(tablename,match.group('date'),int(match.group('uin')),int(match.group('roomid')),"",int(match.group('time')))
		commentstr = u"房间号:%d,观看时长:%s" % (int(match.group('roomid')),GetTimeStr(match.group('time')))
		eventsql = EvensIDS.GetEventSql(EvensIDS.EVENT_LOGOUT_ID,int(match.group('uin')),match.group('date'),commentstr)
	sqllist['insert'] = insertsql
	sqllist['event'] = eventsql
	return 	sqllist

def  splitTerminateVideo(message):
	insertsql = None
	eventsql = None
	sqllist = {}
	global imquokkaDBsession
	regstr = '(?P<date>\d+/\d+/\d+\s+\d+:\d+:\d+)\|(?:\s+roomid=)(?P<roomid>\d+)(?:\s+update\s+user\s+vid\s+)(?P<vid>\d+_\d+)(?:\s+viewNum=)(?P<viewnum>\d+)(?:\s+viewTime=)(?P<viewtime>\d+)(?:\s+duration\s+)(?P<duration>\d+)'\
			'(?:\s+laudCount\s+)(?P<laudcount>\d+)(?:\s+vState\s+)(?P<vstate>\d+)(?:\s+webCurrNum=\d+\s+currNum=)(?P<currnum>\d+)'
	pattern =re.compile(regstr)
	match = pattern.search(message.value['message'])
	if match:
		if (int(match.group('vstate')) == 1) or (int(match.group('vstate'))==5):
			tablename= TableNameS.suibo_usr_closevideo + '_' + time.strftime("%Y%m", time.localtime())
			insertsql = "insert into %s(date,vid,viewnum,laudcount,duration) values('%s','%s',%d,%d,%d)"%\
					(tablename,match.group('date'),match.group('vid'),int(match.group('viewnum')),int(match.group('laudcount')),int(match.group('duration')))
			
			rcvgiftsql = "select sum(money) AS money,count(1) AS num,srcUin" \
				" from %s WHERE  vid ='%s' GROUP BY srcUin" %(TableNameS.suibo_room_sendgift_info,match.group('vid'))
			retresult = session.excute(rcvgiftsql)
			recvemoney = 0
			recvcount = 0
			senderuincount = 0
			maxnumsenduin = 0
			maxmoneysenduin = 0
			maxnum = 0
			maxmoney = 0
			if retresult!=None:
				senderuincount =  retresult.rowcount
				if senderuincount>0:
					rows = retresult.fetchall()
					for row in rows:
						recvemoney =  recvemoney + row['money']
						recvcount = recvcount + row['num']
						if maxnum <  int(row['num']):
							maxnum = int(row['num'])
							maxnumsenduin = int(row['srcUin'])
						if maxmoney < int (row['money']):
							maxmoneysenduin = int(row['srcUin'])
							maxmoney = int( row['money'])

			rewardcommentstr=""
			if int(match.group('duration'))>0:
				sumdatablename = TableNameS.bo_sumdayvid + '_' + time.strftime("%Y%m", time.localtime())
				rewartsql = "select amount,acash,kind from %s WHERE vid IN('%s') AND kind IN(10,186,187,188,189)" %(sumdatablename,match.group('vid'))
				rewardret = imquokkaDBsession.excute(rewartsql)
				if rewardret!=None:
					rewardrows = rewardret.fetchall()
					for rewardrow in rewardrows:
						if int(rewardrow['kind']) == 186:
							temstr =  u'获得直播奖励%f人民币' %(rewardrow['amount']/float(100000))
							rewardcommentstr = rewardcommentstr + temstr
						elif int(int(rewardrow['kind']) == 189):
							temstr =  u'获得首播奖励%f人民币 ' %(rewardrow['amount']/float(100000))
							rewardcommentstr = rewardcommentstr + temstr
						elif int(int(rewardrow['kind']) == 188):
							temstr =  u'分享直播奖励%f人民币 ' %(rewardrow['amount']/float(100000))
							rewardcommentstr = rewardcommentstr + temstr
						elif int(int(rewardrow['kind']) == 187):
							temstr =  u'观看奖励%f人民币 ' %(rewardrow['amount']/float(100000))
							rewardcommentstr = rewardcommentstr + temstr
						elif int(int(rewardrow['kind']) == 10):
							temstr =  u'收礼奖励%f人民币 ' %(rewardrow['amount']/float(100000))
							rewardcommentstr = rewardcommentstr + temstr
					#print rewardcommentstr
			recvgiftcomment = u'总收礼次数:%d,总送礼人数:%d,总乐豆:%d(%f人民币) 送礼次数最多用户:%d %d次 送礼总金额最多用户:%d %d乐豆(%f人民币)' %\
								(recvcount,senderuincount,recvemoney,float(recvemoney)/float(100000),maxnumsenduin,maxnum,maxmoneysenduin,maxmoney,float(maxmoney)/float(100000))
			commentstr = u"vid:%s,开播时长:%s,累积观看时长:%s,总观看人数:%d,总点赞数:%d %s %s" % \
						(match.group('vid'),GetTimeStr(match.group('duration')),GetTimeStr(match.group('viewtime')),\
						int(match.group('viewnum')),int(match.group('laudcount')),recvgiftcomment,rewardcommentstr)


			vidtemplst = match.group('vid').split('_')
			eventsql = EvensIDS.GetEventSql(EvensIDS.EVENT_TEMINATEVIDEO_ID,int(vidtemplst[0]),match.group('date'),commentstr)
	sqllist['insert'] = insertsql
	sqllist['event'] = eventsql
	return 	sqllist

def Split():
	global kafka_hosts
	global session
	consumer = KafkaConsumer('suiboltlog',
						 group_id='suibo',
                         client_id="suibochat",
                         bootstrap_servers=kafka_hosts,value_deserializer=lambda m: json.loads(m.decode('utf-8')),auto_offset_reset="earliest", enable_auto_commit=True)
	for message in consumer:
		sqllist = splitChat(message)
		if sqllist.has_key('insert') and sqllist['insert']!= None:
			session.excute(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				session.excute(sqllist['event'])
			continue
		sqllist = splitSendGift(message)
		if sqllist.has_key('insert') and sqllist['insert']!= None:
			session.excute(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				session.excute(sqllist['event'])
			if sqllist.has_key('event2') and sqllist['event2']!=None:
				session.excute(sqllist['event2'])
			continue
		sqllist = splitLL(message)
		if sqllist.has_key('insert') and sqllist['insert']!= None:
			session.excute(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				session.excute(sqllist['event'])
			continue

		sqllist = splitLO(message)
		if sqllist.has_key('insert') and sqllist['insert']!= None:
			session.excute(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				session.excute(sqllist['event'])
			continue

		sqllist = splitTerminateVideo(message)
		if sqllist.has_key('insert') and sqllist['insert']!= None:
			session.excute(sqllist['insert'])
			if sqllist.has_key('event') and sqllist['event']!=None:
				session.excute(sqllist['event'])
			continue


def main():
	taolelogs.InitailLogs('suibochat')
	InitialDB()
	InitialimquokkaDB()
	Split()

if __name__ == "__main__":
	main()
