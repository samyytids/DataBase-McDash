import sqlite3
import pandas as pd
import os 
from dataclasses import dataclass
import math as maths 

# by using chunk size I can open files in parts in order to not destroy my computer 
chunksize = 10 ** 6

mcdash_directory = os.fsencode(r'C:\Users\STAJa\Desktop\McPython')

@dataclass
class McDashFullVariables:
    Loanid: str
    year: int
    origination_year: int
    AsOfMonth: int
    LoanAge: int
    InterestType: int
    InterestRate: float
    PriorInterestRate: float
    UPB: int
    NextPaymentDueMonth: int
    Foreclosureid: int
    BankruptcyFlag: int
    PIFid: int
    Investorid: int
    PandIConstant: int
    PaymentStatus: str
    TAndIConstant: int
    SPB: int    
    HasPriorMonth: int
    UpdateDtTm: str
    CurrentJumboFlag: int
    UPBtoSPB: float
    PriorPaymentStatus: str

@dataclass
class McDashMisc:
    Loanid: str
    AsOfMonth: int
    BankruptcyChapterid: int
    InLossMitigation: int
    CurrentCreditScore: int
    RemainingTerm: int

@dataclass
class McDashPayment:
    Loanid: str  
    AsOfMonth: int
    IsNegAmortPayment: int
    NegAmortAmount: int
    IsPrepayment: int
    PrepaymentAmount: int
    IsIOPayment: int
    ChargeOffAmount: int

conn = sqlite3.connect('McDash.db')
cursor = conn.cursor()

try:
    cursor.execute("""CREATE TABLE FullVariables
    (
    Loanid str,
    year int,
    origination_year int,
    AsOfMonth int,
    LoanAge int,
    InterestType float,
    InterestRate float,
    PriorInterestRate int,
    UPB int,
    NextPaymentDueMonth int,
    Foreclosureid int,
    BankruptcyFlag int,
    PIFid int,
    Investorid int,
    PandIConstant int,
    PaymentStatus str,
    TAndIConstant int,
    SPB int,
    HasPriorMonth int,
    UpdateDtTm str,
    CurrentJumboFlag int,
    UPBtoSPB float,
    PriorPaymentStatus str,
    UNIQUE (Loanid, AsOfMonth)
    )""")
except:
    pass

try:
    cursor.execute("""CREATE TABLE Payment
    (
    Loanid str, 
    AsOfMonth int,
    IsNegAmortPayment int,
    NegAmortAmount int,
    IsPrepayment int,
    PrepaymentAmount int,
    IsIOPayment int,
    ChargeOffAmount int,
    UNIQUE (Loanid, AsOfMonth)
    )""")
except:
    pass


try:
    cursor.execute("""CREATE TABLE Misc
    (
    Loanid str,
    AsOfMonth int,
    BankruptcyChapterid int,
    InLossMitigation int,
    CurrentCreditScore int,
    RemainingTerm int,
    UNIQUE (Loanid, AsOfMonth)
    )""")
except:
    pass

conn.commit()            
conn.close()

for file in os.listdir(mcdash_directory):
    file = str(file).replace('b', '').replace("'", '')
    with pd.read_csv(rf'C:\Users\STAJa\Desktop\McPython\{file}', chunksize=chunksize) as reader:
        for chunk in reader:
            print(file)
            conn = sqlite3.connect('McDash.db')
            cursor = conn.cursor()

            full_variables = McDashFullVariables(
                Loanid = chunk['Loanid'],
                year = chunk['year'],
                origination_year = chunk['origination_year'],
                AsOfMonth = chunk['AsOfMonth'],
                LoanAge = chunk['LoanAge'],
                InterestType = chunk['InterestType'],
                InterestRate = chunk['InterestRate'],
                PriorInterestRate = chunk['PriorInterestRate'], 
                UPB = chunk['UPB'],
                NextPaymentDueMonth = chunk['NextPaymentDueMonth'],
                Foreclosureid = chunk['Foreclosureid'],
                BankruptcyFlag = chunk['BankruptcyFlag'],
                PIFid = chunk['PIFid'],
                Investorid = chunk['Investorid'],
                PandIConstant = chunk['PandIConstant'],
                PaymentStatus = chunk['PaymentStatus'],
                TAndIConstant = chunk['TAndIConstant'],
                SPB = chunk['SPB'],
                HasPriorMonth = chunk['HasPriorMonth'],
                UpdateDtTm = chunk['UpdateDtTm'],
                CurrentJumboFlag = chunk['CurrentJumboFlag'],
                PriorPaymentStatus = chunk['PriorPaymentStatus'],
                UPBtoSPB = chunk['UPBtoSPB'])

            payment_variables = McDashPayment(
                Loanid = chunk['Loanid'],     
                AsOfMonth = chunk['AsOfMonth'], 
                IsNegAmortPayment = chunk['IsNegAmortPayment'], 
                NegAmortAmount = chunk['NegAmortAmount'], 
                IsPrepayment = chunk['IsPrepayment'], 
                PrepaymentAmount = chunk['PrepaymentAmount'], 
                IsIOPayment = chunk['IsIOPayment'], 
                ChargeOffAmount = chunk['ChargeOffAmount']
            )

            misc_variables = McDashMisc(
                Loanid = chunk['Loanid'],
                AsOfMonth = chunk['AsOfMonth'],
                BankruptcyChapterid = chunk['BankruptcyChapterid'],
                InLossMitigation = chunk['InLossMitigation'],
                CurrentCreditScore = chunk['CurrentCreditScore'],
                RemainingTerm = chunk['RemainingTerm']
            )
            
        cursor.execute(f"""INSERT OR IGNORE INTO FullVariables VALUES
        (
            '{full_variables.Loanid}',
            '{full_variables.year}',
            '{full_variables.origination_year}',
            '{full_variables.AsOfMonth}',
            '{full_variables.LoanAge}',
            '{full_variables.InterestType}',
            '{full_variables.InterestRate}',
            '{full_variables.PriorInterestRate}',
            '{full_variables.UPB}',
            '{full_variables.NextPaymentDueMonth}',
            '{full_variables.Foreclosureid}',
            '{full_variables.BankruptcyFlag}',
            '{full_variables.PIFid}',
            '{full_variables.Investorid}',
            '{full_variables.PandIConstant}',
            '{full_variables.PaymentStatus}',
            '{full_variables.TAndIConstant}',
            '{full_variables.SPB}',
            '{full_variables.HasPriorMonth}',
            '{full_variables.UpdateDtTm}',
            '{full_variables.CurrentJumboFlag}',
            '{full_variables.PriorPaymentStatus}',
            '{full_variables.UPBtoSPB}'
        )""")

        cursor.execute(f"""INSERT OR IGNORE INTO Payment VALUES
        (
            '{payment_variables.Loanid}',
            '{payment_variables.AsOfMonth}',
            '{payment_variables.IsNegAmortPayment}',
            '{payment_variables.NegAmortAmount}',
            '{payment_variables.IsPrepayment}',
            '{payment_variables.PrepaymentAmount}',
            '{payment_variables.IsIOPayment}',
            '{payment_variables.ChargeOffAmount}'
        )""")

        cursor.execute(f"""INSERT OR IGNORE INTO Misc VALUES
        (
            '{misc_variables.Loanid}',
            '{misc_variables.AsOfMonth}',
            '{misc_variables.BankruptcyChapterid}',
            '{misc_variables.InLossMitigation}',
            '{misc_variables.CurrentCreditScore}',
            '{misc_variables.RemainingTerm}'
        )""")

        conn.commit()            
        conn.close()


                