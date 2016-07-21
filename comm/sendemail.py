# encoding: utf-8
#!/usr/bin/python
import smtplib
from email.mime.text import MIMEText
from email.header import Header
def sendEmail(subject,tousrs,fromstr,msg):
	sender = 'taolesoft@163.com'
	smtpserver = 'smtp.exmail.qq.com'
	username = 'taolesoft@163.com'
	password = 'hztaole0575123'
	msg = MIMEText(msg, 'plain', 'utf-8')
	msg['Subject'] = Header(subject, 'utf-8')
	msg['from'] = fromstr
	msg['to'] = ",".join(tousrs)
	smtp = smtplib.SMTP()
	smtp.connect('smtp.163.com')
	smtp.login(username, password)
	smtp.sendmail(sender, tousrs, msg.as_string())
	smtp.quit()

#sendEmail('fdf',['whg@taolesoft.com','76980374@qq.com'],'dgdfg','报')