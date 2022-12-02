import sqlite3
import pandas as pd
import os 
from dataclasses import dataclass, field
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
    PaymentStatus: int
    TAndIConstant: int
    SPB: int    
    HasPriorMonth: int
    UpdateDtTm: str
    CurrentJumboFlag: int
    UPBtoSPB: float
    PriorPaymentStatus: str
    Trueid: str = field(init = False)

    def __post_init__(self):
        self.Trueid = f"{self.Loanid}M{self.AsOfMonth}"


@dataclass
class McDashMisc:
    Loanid: str
    AsOfMonth: int
    BankruptcyChapterid: int
    InLossMitigation: int
    CurrentCreditScore: int
    RemainingTerm: int
    Trueid: str = field(init = False)

    def __post_init__(self):
        self.Trueid = f"{self.Loanid}M{self.AsOfMonth}"

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
    Trueid: str = field(init = False)

    def __post_init__(self):
        self.Trueid = f"{self.Loanid}M{self.AsOfMonth}"

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
    Trueid str,
    UNIQUE(Trueid)
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
    Trueid str,
    UNIQUE(Trueid)
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
    Trueid str,
    UNIQUE(Trueid)
    )""")
except:
    pass

conn.commit()            
conn.close()

def create_database():
    for file in os.listdir(mcdash_directory):
        file = str(file).replace('b', '').replace("'", '')
        with pd.read_csv(rf'C:\Users\STAJa\Desktop\McPython\{file}', chunksize=chunksize) as reader:
            for chunk in reader:
                print(file)

                conn = sqlite3.connect('McDash.db')
                cursor = conn.cursor()

                full_variables = McDashFullVariables(
                    Loanid = chunk['Loanid'].values,
                    year = chunk['year'].values,
                    origination_year = chunk['origination_year'].values,
                    AsOfMonth = chunk['AsOfMonth'].values,
                    LoanAge = chunk['LoanAge'].values,
                    InterestType = chunk['InterestType'].values,
                    InterestRate = chunk['InterestRate'].values,
                    PriorInterestRate = chunk['PriorInterestRate'].values,
                    UPB = chunk['UPB'].values,
                    NextPaymentDueMonth = chunk['NextPaymentDueMonth'].values,
                    Foreclosureid = chunk['Foreclosureid'].values,
                    BankruptcyFlag = chunk['BankruptcyFlag'].values,
                    PIFid = chunk['PIFid'].values,
                    Investorid = chunk['Investorid'].values,
                    PandIConstant = chunk['PandIConstant'].values,
                    PaymentStatus = chunk['PaymentStatus'].values,
                    TAndIConstant = chunk['TAndIConstant'].values,
                    SPB = chunk['SPB'].values,
                    HasPriorMonth = chunk['HasPriorMonth'].values,
                    UpdateDtTm = chunk['UpdateDtTm'].values,
                    CurrentJumboFlag = chunk['CurrentJumboFlag'].values,
                    PriorPaymentStatus = chunk['PriorPaymentStatus'].values,
                    UPBtoSPB = chunk['UPBtoSPB'].values)

                payment_variables = McDashPayment(
                    Loanid = chunk['Loanid'].values,     
                    AsOfMonth = chunk['AsOfMonth'].values, 
                    IsNegAmortPayment = chunk['IsNegAmortPayment'].values, 
                    NegAmortAmount = chunk['NegAmortAmount'].values, 
                    IsPrepayment = chunk['IsPrepayment'].values, 
                    PrepaymentAmount = chunk['PrepaymentAmount'].values, 
                    IsIOPayment = chunk['IsIOPayment'].values, 
                    ChargeOffAmount = chunk['ChargeOffAmount'].values
                )

                misc_variables = McDashMisc(
                    Loanid = chunk['Loanid'].values,
                    AsOfMonth = chunk['AsOfMonth'].values,
                    BankruptcyChapterid = chunk['BankruptcyChapterid'].values,
                    InLossMitigation = chunk['InLossMitigation'].values,
                    CurrentCreditScore = chunk['CurrentCreditScore'].values,
                    RemainingTerm = chunk['RemainingTerm'].values
                )
                
                print(full_variables.PaymentStatus)
                
            cursor.execute(f'''INSERT OR IGNORE INTO FullVariables VALUES
            (
                "{full_variables.Loanid}",
                "{full_variables.year}",
                "{full_variables.origination_year}",
                "{full_variables.AsOfMonth}",
                "{full_variables.LoanAge}",
                "{full_variables.InterestType}",
                "{full_variables.InterestRate}",
                "{full_variables.PriorInterestRate}",
                "{full_variables.UPB}",
                "{full_variables.NextPaymentDueMonth}",
                "{full_variables.Foreclosureid}",
                "{full_variables.BankruptcyFlag}",
                "{full_variables.PIFid}",
                "{full_variables.Investorid}",
                "{full_variables.PandIConstant}",
                "{full_variables.PaymentStatus}",
                "{full_variables.TAndIConstant}",
                "{full_variables.SPB}",
                "{full_variables.HasPriorMonth}",
                "{full_variables.UpdateDtTm}",
                "{full_variables.CurrentJumboFlag}",
                "{full_variables.PriorPaymentStatus}",
                "{full_variables.UPBtoSPB}",
                "{full_variables.Trueid}"
            )''')

            cursor.execute(f"""INSERT OR IGNORE INTO Payment VALUES
            (
                '{payment_variables.Loanid}',
                '{payment_variables.AsOfMonth}',
                '{payment_variables.IsNegAmortPayment}',
                '{payment_variables.NegAmortAmount}',
                '{payment_variables.IsPrepayment}',
                '{payment_variables.PrepaymentAmount}',
                '{payment_variables.IsIOPayment}',
                '{payment_variables.ChargeOffAmount}',
                '{full_variables.Trueid}'
            )""")

            cursor.execute(f"""INSERT OR IGNORE INTO Misc VALUES
            (
                '{misc_variables.Loanid}',
                '{misc_variables.AsOfMonth}',
                '{misc_variables.BankruptcyChapterid}',
                '{misc_variables.InLossMitigation}',
                '{misc_variables.CurrentCreditScore}',
                '{misc_variables.RemainingTerm}',
                '{full_variables.Trueid}'
            )""")

            conn.commit()            
            conn.close()


if __name__ == "__main__":
    create_database()

    conn = sqlite3.connect('McDash.db')
    cursor = conn.cursor()      

    test = cursor.execute("SELECT * FROM Misc").fetchall()
    headers = [i[1] for i in cursor.execute("PRAGMA table_info(Misc)").fetchall()]
    test = pd.DataFrame(test)
    print(test[0][0])


    conn.commit()
    conn.close()
