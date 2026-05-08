import smtplib
import logging
from email.message import EmailMessage

def send_summary_email(smtp_server, smtp_port, mail_from, mail_to, mail_cc, subject, body):
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = mail_from
    
    to_list = [e.strip() for e in mail_to.split(',') if e.strip()]
    cc_list = [e.strip() for e in mail_cc.split(',') if e.strip()] if mail_cc else []
    
    msg['To'] = ", ".join(to_list)
    if cc_list:
        msg['Cc'] = ", ".join(cc_list)

    all_recipients = to_list + cc_list

    try:
        with smtplib.SMTP(smtp_server, int(smtp_port)) as server:
            server.send_message(msg)
        logging.info(f"ส่งอีเมลสำเร็จ -> To: {mail_to} | CC: {mail_cc}")
        return True
    except Exception as e:
        logging.error(f"เกิดข้อผิดพลาดในการส่งอีเมล: {e}")
        return False
