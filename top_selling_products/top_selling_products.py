import pandas as pd
from sqlalchemy import create_engine
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import config

#---------- DATABASE CONNECTIONS ---------- #
server = 'azbiprodasw02-ondemand.sql.azuresynapse.net'
database = 'reporting'
username = 'slussier@jamppharma.com'

# Connection string for SQLAlchemy with Azure Active Directory authentication
connection_string = f"mssql+pyodbc://{username}:YourPasswordHere@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server&Authentication=ActiveDirectoryInteractive&Encrypt=yes"

#---------- SQL Query ---------- #
# Create the SQLAlchemy engine
engine = create_engine(connection_string)

query = """
WITH MaxDate AS (
    SELECT 
        MAX(SalDate) AS MaxSalDate,
        CASE
            WHEN MONTH(MAX(SalDate)) BETWEEN 4 AND 6 THEN 'Q1'
            WHEN MONTH(MAX(SalDate)) BETWEEN 7 AND 9 THEN 'Q2'
            WHEN MONTH(MAX(SalDate)) BETWEEN 10 AND 12 THEN 'Q3'
            WHEN MONTH(MAX(SalDate)) BETWEEN 1 AND 3 THEN 'Q4'
        END AS MaxSalQuarter
    FROM vw_fact_gss_gross_sales
),

MonthlySales AS (
    SELECT
        ProdUpc,
        ProdDescription,
        SalName,
        CusNumber,
        CusName,
        SUM(SalSales) AS MonthlyTotalSales,
        CASE
            WHEN MONTH(SalDate) BETWEEN 4 AND 6 THEN 'Q1'
            WHEN MONTH(SalDate) BETWEEN 7 AND 9 THEN 'Q2'
            WHEN MONTH(SalDate) BETWEEN 10 AND 12 THEN 'Q3'
            WHEN MONTH(SalDate) BETWEEN 1 AND 3 THEN 'Q4'
        END AS SalQuarter
    FROM
        vw_fact_gss_gross_sales
    WHERE
        SalDate = (SELECT MaxSalDate FROM MaxDate)
    GROUP BY
        ProdUpc,
        ProdDescription,
        SalName,
        CusNumber,
        CusName,
        CASE
            WHEN MONTH(SalDate) BETWEEN 4 AND 6 THEN 'Q1'
            WHEN MONTH(SalDate) BETWEEN 7 AND 9 THEN 'Q2'
            WHEN MONTH(SalDate) BETWEEN 10 AND 12 THEN 'Q3'
            WHEN MONTH(SalDate) BETWEEN 1 AND 3 THEN 'Q4'
        END
),

QuarterlySales AS (
    SELECT
        ProdUpc,
        ProdDescription,
        SalName,
        CusNumber,
        CusName,
        SUM(SalSales) AS QuarterlyTotalSales,
        CASE
            WHEN MONTH(SalDate) BETWEEN 4 AND 6 THEN 'Q1'
            WHEN MONTH(SalDate) BETWEEN 7 AND 9 THEN 'Q2'
            WHEN MONTH(SalDate) BETWEEN 10 AND 12 THEN 'Q3'
            WHEN MONTH(SalDate) BETWEEN 1 AND 3 THEN 'Q4'
        END AS SalQuarter
    FROM
        vw_fact_gss_gross_sales
    WHERE
        SalDate = (SELECT MaxSalDate FROM MaxDate)
        AND CASE
            WHEN MONTH(SalDate) BETWEEN 4 AND 6 THEN 'Q1'
            WHEN MONTH(SalDate) BETWEEN 7 AND 9 THEN 'Q2'
            WHEN MONTH(SalDate) BETWEEN 10 AND 12 THEN 'Q3'
            WHEN MONTH(SalDate) BETWEEN 1 AND 3 THEN 'Q4'
        END = (SELECT MaxSalQuarter FROM MaxDate)
    GROUP BY
        ProdUpc,
        ProdDescription,
        SalName,
        CusNumber,
        CusName,
        CASE
            WHEN MONTH(SalDate) BETWEEN 4 AND 6 THEN 'Q1'
            WHEN MONTH(SalDate) BETWEEN 7 AND 9 THEN 'Q2'
            WHEN MONTH(SalDate) BETWEEN 10 AND 12 THEN 'Q3'
            WHEN MONTH(SalDate) BETWEEN 1 AND 3 THEN 'Q4'
        END
)

SELECT
    ms.SalName AS Rep,
    CONCAT(r.DescripJamp, ' ', r.ForceStnd, ' ', r.PackType, ' ', r.PackStnd) AS Product,
    ms.CusNumber,
    ms.CusName,
    FORMAT(ms.MonthlyTotalSales, 'C', 'en-CA') AS MonthlyTotalSales,
    FORMAT(qs.QuarterlyTotalSales, 'C', 'en-CA') AS QuarterlyTotalSales
FROM
    MonthlySales ms
JOIN
    QuarterlySales qs ON ms.ProdUpc = qs.ProdUpc
    AND ms.ProdDescription = qs.ProdDescription
    AND ms.SalName = qs.SalName
    AND ms.CusName = qs.CusName
JOIN
    vw_ref_iqvia_standart_table r ON ms.ProdUpc = r.UPC_GRP_JAMP
WHERE
    (ms.MonthlyTotalSales >= 20000 OR qs.QuarterlyTotalSales >= 60000)
    AND ms.ProdDescription NOT IN ('Simlandi Injectable', 'Jamteki Injectable')
    AND ms.SalName NOT LIKE '%Vacant%'
    AND ms.SalName NOT LIKE '%Institutional%'
    AND ms.SalName NOT IN ('AB - Retail Key Accounts', 'Alexander Winton', 'Benoit Poupart - Temporaire', 'Brian Murphy', 'Carol Steel', 'Carol Steel - Exception Accounts', 'Cédric-Gagné Marcoux (SPÉCIAL)', 'Jade Budd - Exception Accounts', 'Josh Foreman', 'Lovell', 'Marianne Calamia', 'MB - Retail Key Accounts', 'Neighbourly Pharmacy (Chris Gardner)', 'Ross Miller', 'Target', 'Zellers', 'Rubicon Pharmacies', 'Ontario - Other Retailers')
ORDER BY
    ms.MonthlyTotalSales DESC;

"""

# Load query results into a pandas DataFrame
df = pd.read_sql(query, engine)
df.to_excel('top_selling_products.xlsx', index=False, engine='openpyxl')

print("Data retrieval complete!")


#---------- EMAIL ---------- #
sender_email = config.SMTP_USER
receiver_email = "shayne@shaynelussier.com"  # Or any other email you want to send to
subject = "Top Selling Products Report"
body = "Please find attached the top selling products report."

# Titan SMTP server settings from config.py, USE ENVIRONMENT VARIABLES
smtp_server = config.SMTP_SERVER
smtp_port = config.SMTP_PORT
smtp_user = config.SMTP_USER
smtp_password = config.SMTP_PASSWORD

# Create the email message
msg = MIMEMultipart()
msg["From"] = sender_email
msg["To"] = receiver_email
msg["Subject"] = subject
msg.attach(MIMEText(body, 'plain'))

# Attach the Excel file
file_path = 'top_selling_products.xlsx'
with open(file_path, 'rb') as attachment:
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename={file_path}')
    msg.attach(part)

# Set up the SMTP server and send the email
try:
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()  # Start TLS encryption
        server.login(smtp_user, smtp_password)  # Log in with your credentials
        server.sendmail(sender_email, receiver_email, msg.as_string())  # Send email
        print("Email sent successfully!")
except Exception as e:
    print(f"Error: {e}")
