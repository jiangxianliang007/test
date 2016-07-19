#coding=utf-8
import time
from tabledefine import TableNameS
class EvensIDS(object):
	#'事件类型说明'
	EVENT_CHANGENAME_ID = 1 #更改昵称
	EVENT_LOGIN_ID = 2		#授权登录
	EVENT_HONGBAO_ID = 3	#红包分享
	EVENT_LOGINCHAT_ID = 4  #登录直播房间
	EVENT_SENDGIFT_ID = 5  #送礼:
	EVENT_LOGOUT_ID = 6    #退出房间
	EVENT_TEMINATEVIDEO_ID = 7#关掉直播
	EVENT_BUY_ID = 8		#充值
	EVENT_DRAWMONEY_ID = 9	#提现

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
		elif ntype == EvensIDS.EVENT_BUY_ID:
			return u"充值:"
		elif ntype == EvensIDS.EVENT_DRAWMONEY_ID:
			return u"提现:"

	@staticmethod
	def GetEventSql(neid,nuin,sdate,scommnet):
		tablename= TableNameS.suibo_events + '_' + time.strftime("%Y%m", time.localtime())
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

class PayTypeName(object):
	@staticmethod
	def GetName(ntype):
		ntype = int(ntype)
		if ntype ==1:
			return u'财富通'
		elif ntype == 2:
			return u'支付宝'
		elif ntype == 4:
			return u'微信'
		elif ntype == 5:
			return u'Apple'
		return ''