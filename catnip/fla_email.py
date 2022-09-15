from dataclasses import dataclass
from typing import List

from prefect.client import Secret 

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import os


@dataclass
class FLA_Email:

    sender: str = Secret("PI_MICROSOFT_USERNAME_EMAIL").get()
    sender_pw: str = Secret("PI_MICROSOFT_PASSWORD").get()

    receiver: str = "mcgoyen@floridapanthers.com" # pass in single address

    subject: str = ""
    body: str = ""         # HTML string
    cc: List = None         # pass in single address or list of addresses
    
    attachments : List[str] = None

    def send_email(self) -> None:

        cc_stringlist = ", ".join(self.cc) if self.cc is not None else [""]

        message = MIMEMultipart()

        message['From'] = self.sender
        message['To'] = self.receiver
        message['Subject'] = self.subject
        message['Cc'] = cc_stringlist
        to_addrs = [self.receiver] + cc_stringlist

        message.attach(MIMEText(self.body, "html"))


        '''
            - Need to update attachement functionality to accomodate interacting with SharePoint
            - ? submit sharepoint path -> download into tempfile location -> open as stream and attach ?
        
        '''
        # for filepath in self.attachments:
        #     with open(filepath, "rb") as attachment:
        #         part = MIMEBase("application", "octet-stream")
        #         part.set_payload(attachment.read())

        #     encoders.encode_base64(part)
        #     filename = os.path.basename(filepath)
        #     part.add_header(
        #         "Content-Disposition",
        #         f"attachment; filename= {filename}",
        #     )
        #     message.attach(part)


        text = message.as_string()

        with smtplib.SMTP("smtp-mail.outlook.com", 587) as server:
            server.starttls()
            server.login(self.sender, self.sender_pw)
            server.sendmail(self.sender, to_addrs, text)
            server.quit()

        return None