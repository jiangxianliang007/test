# encoding: utf-8
#!/usr/bin/python
import os
import smtplib
from email.mime.text import MIMEText
from email.header import Header


DEFAULT_SMTP_SERVER = 'smtp.exmail.qq.com'
DEFAULT_SMTP_USER = 'redmine@taolesoft.com'


def sendEmail(subject, tousrs, fromstr, msg):
	"""Send an email using credentials supplied through the environment."""
	smtpserver = os.environ.get('SMTP_SERVER', DEFAULT_SMTP_SERVER)
	username = os.environ.get('SMTP_USERNAME', DEFAULT_SMTP_USER)
	password = os.environ.get('SMTP_PASSWORD')
	if not password:
		raise ValueError('SMTP_PASSWORD environment variable is required')

	message = MIMEText(msg, 'plain', 'utf-8')
	message['Subject'] = Header(subject, 'utf-8')
	message['From'] = fromstr
	message['To'] = ",".join(tousrs)

	smtp = smtplib.SMTP(smtpserver)
	try:
		smtp.login(username, password)
		smtp.sendmail(username, tousrs, message.as_string())
	finally:
		smtp.quit()
