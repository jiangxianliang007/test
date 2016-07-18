#coding=utf-8
import time
class EvensIDS(object):
	#'事件类型说明'
	EVENT_CHANGENAME_ID = 1
	EVENT_LOGIN_ID = 2
	EVENT_HONGBAO_ID = 3
	EVENT_LOGINCHAT_ID = 4
	EVENT_SENDGIFT_ID = 5
	EVENT_LOGOUT_ID = 6
	EVENT_TEMINATEVIDEO_ID = 7
	@staticmethod
	def GetEventName(ntype):
		if ntype == EvensIDS.EVENT_CHANGENAME_ID:
			return u"更改昵称:"
		elif ntype == EvensIDS.EVENT_LOGIN_ID:
			return u"授权登录:"
		elif ntype == EvensIDS.EVENT_HONGBAO_ID:
			return u"红包分享"
		elif ntype == EvensIDS.EVENT_LOGINCHAT_ID:
			return u"登录直播房间:"
		elif ntype == EvensIDS.EVENT_SENDGIFT_ID:
			return u"送礼:"
		elif ntype == EvensIDS.EVENT_LOGOUT_ID:
			return u"退出房间:"
		elif ntype == EvensIDS.EVENT_TEMINATEVIDEO_ID:
			return u"结束直播:"

	@staticmethod
	def GetEventSql(neid,nuin,sdate,scommnet):
		tablename= "suibo_events_" + time.strftime("%Y%m", time.localtime())
		eventstr = "insert into %s (date,eid,uin,comment) values('%s',%d,%d,'%s') "%(tablename,sdate,int(neid),int(nuin),scommnet)
		return eventstr



class  LoginType(object):
	#"1:微信 2:QQ 3:微博 4:facebook 5:twitter 99:授权"
	logintype ={'1':u'微信','2':u'qq','3':u'微博','4':u'facebook','5':u'twitter','6':u'淘乐pad','99':u'手机'}
	@staticmethod
	def GetName(ntype):
		retstr = u'未知'
		if LoginType.logintype.has_key(str(ntype)):
			retstr =LoginType.logintype[str(ntype)]
		return retstr
