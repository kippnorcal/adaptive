from smtplib import SMTP_SSL
from os import getenv
from email.mime.text import MIMEText


class Mailer:
    def __init__(self):
        self.user = getenv("GMAIL_USER")
        self.password = getenv("GMAIL_PWD")
        self.slack_email = getenv("SLACK_EMAIL")
        self.server = SMTP_SSL("smtp.gmail.com", 465)
        self.from_address = "KIPP NorCal Job Notification"
        self.to_address = "databot"

    def _subject_line(self):
        """Job status notification - subject line."""
        subject_type = "Success" if self.success else "Error"
        return f"Adaptive - {subject_type}"

    def _body_text(self):
        """Job status notification - body text."""
        if self.success:
            return f"The Adaptive job ran successfully.\n{self.logs}"
        else:
            return f"The Adaptive job encountered an error:\n{self.logs}"

    def _message(self):
        msg = MIMEText(self._body_text())
        msg["Subject"] = self._subject_line()
        msg["From"] = self.from_address
        msg["To"] = self.to_address
        return msg.as_string()

    def _read_logs(self, filename):
        with open(filename) as f:
            return f.read()

    def notify(self, success):
        self.success = success
        self.logs = self._read_logs("app.log")
        with self.server as s:
            s.login(self.user, self.password)
            msg = self._message()
            s.sendmail(self.user, self.slack_email, msg)