import logging
import traceback
import pyodbc
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate
import smtplib


# SQL HANDLER
# log_manager/custom_handlers.py (SQLServerHandler)
class SQLServerHandler(logging.Handler):
    def __init__(self, server, driver, user, password, database, table):
        super().__init__()
        self.server = server
        self.driver = driver
        self.user = user
        self.password = password
        self.database = database
        self.table = table
        self.conn = None
        self.cursor = None
        # DO NOT call self.connect() here

    def connect(self):
        if self.conn:
            return
        try:
            self.conn = pyodbc.connect(
                f"DRIVER={self.driver};SERVER={self.server};UID={self.user};PWD={self.password};DATABASE={self.database}"
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            # Don’t raise here; let emit handle it gracefully
            print(f"Failed to connect to database for logging: {e}")
            self.conn = None
            self.cursor = None

    def emit(self, record):
        if not self.conn:
            self.connect()
        if not self.conn:
            # Couldn’t connect—fail soft so console logs still work
            return

        try:
            sql = f"""INSERT INTO {self.table}
                      (LevelName, ModuleName, Message, PathName, FunctionName, [LineNo], Exception, Stack, Created)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            self.cursor.execute(
                sql,
                (
                    record.levelname,
                    record.name,
                    record.getMessage(),
                    record.pathname,
                    record.funcName,
                    record.lineno,
                    self.format_exception(record.exc_info) if record.exc_info else None,
                    record.stack_info if record.stack_info else None,
                    self.format_timestamp(record.created),
                ),
            )
            self.conn.commit()
        except Exception:
            self.handleError(record)

    def format_exception(self, exc_info):
        if exc_info:
            return "".join(traceback.format_exception(*exc_info))
        else:
            return None

    def format_timestamp(self, timestamp):
        return datetime.datetime.fromtimestamp(timestamp).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )

    def close(self):
        if self.conn:
            self.conn.close()
        super().close()


# EMAIL HANDLER
class CustomSMTPHandler(logging.Handler):
    def __init__(self, mailhost, fromaddr, toaddrs, subject, secure=None, timeout=5.0):
        super().__init__()
        self.mailhost = mailhost
        self.fromaddr = fromaddr
        self.toaddrs = toaddrs
        self.subject = subject
        self.secure = secure
        self.timeout = timeout

    def emit(self, record):
        try:
            # Format the record
            message = self.format(record)
            # Create the email message
            msg = MIMEMultipart()
            msg["From"] = self.fromaddr
            msg["To"] = ",".join(self.toaddrs)
            msg["Date"] = formatdate(localtime=True)
            msg["Subject"] = self.subject

            # Attach the text message
            msg.attach(MIMEText(message, "html"))

            # Send the email
            smtp = smtplib.SMTP(self.mailhost, timeout=self.timeout)

            if self.secure:
                smtp.starttls()
            smtp.sendmail(self.fromaddr, self.toaddrs, msg.as_string())
            smtp.quit()
        except Exception:
            self.handleError(record)
