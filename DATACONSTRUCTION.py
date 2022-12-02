import sqlite3
import pandas as pd
import os 
from dataclasses import dataclass, field
import math as maths 

# by using reader size I can open files in parts in order to not destroy my computer 
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
    OriginalLoanAmount: int
    PrepaymentClauseid: int
    OriginationMonth: int
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

    def remove_zeroes(self):
        if self.IsNegAmortPayment == 0:
            self.IsNegAmortPayment = maths.nan
        if self.NegAmortAmount == 0:
            self.NegAmortAmount = maths.nan
        if self.IsPrepayment == 0:
            self.IsPrepayment = maths.nan
        if self.PrepaymentAmount == 0:
            self.PrepaymentAmount = maths.nan
        if self.IsIOPayment == 0:
            self.PrepaymentAmount = maths.nan
        if self.ChargeOffAmount == 0:
            self.ChargeOffAmount = maths.nan

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
    OriginalLoanAmount int,
    PrepaymentClauseid int,
    OriginationMonth int,
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
    conn = sqlite3.connect('McDash.db')
    cursor = conn.cursor()
    for file in os.listdir(mcdash_directory):
        file = str(file).replace('b', '').replace("'", '')
        with pd.read_csv(rf'C:\Users\STAJa\Desktop\McPython\{file}', chunksize=chunksize) as reader:
            for reader in reader:
                print(file)
                reader = reader.to_numpy()
                for item in reader:


                    full_variables = McDashFullVariables(
                        Loanid = item[0],
                        year = item[31],
                        origination_year = item[32],
                        AsOfMonth = item[1],
                        LoanAge = item[2],
                        InterestType = item[3],
                        InterestRate = item[4],
                        PriorInterestRate = item[5],
                        UPB = item[6],
                        NextPaymentDueMonth = item[7],
                        Foreclosureid = item[8],
                        BankruptcyFlag = item[9],
                        PIFid = item[11],
                        Investorid = item[12],
                        PandIConstant = item[13],
                        PaymentStatus = item[17],
                        TAndIConstant = item[18],
                        SPB = item[19],
                        HasPriorMonth = item[27],
                        UpdateDtTm = item[28],
                        CurrentJumboFlag = item[29],
                        PriorPaymentStatus = item[26],
                        UPBtoSPB = item[30],
                        OriginalLoanAmount = item[34],
                        PrepaymentClauseid = item[36],
                        OriginationMonth = item[37]) 

                    payment_variables = McDashPayment(
                        Loanid = item[0],     
                        AsOfMonth = item[1], 
                        IsNegAmortPayment = item[21], 
                        NegAmortAmount = item[24], 
                        IsPrepayment = item[22], 
                        PrepaymentAmount = item[23], 
                        IsIOPayment = item[20], 
                        ChargeOffAmount = item[25]
                    )

                    misc_variables = McDashMisc(
                        Loanid = item[0],     
                        AsOfMonth = item[1], 
                        BankruptcyChapterid = item[10],
                        InLossMitigation = item[14],
                        CurrentCreditScore = item[15],
                        RemainingTerm = item[16]
                    )                   

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
                        "{full_variables.OriginalLoanAmount}",
                        "{full_variables.PrepaymentClauseid}",
                        "{full_variables.OriginationMonth}", 
                        "{full_variables.Trueid}"
                    )''')

                    payment_variables.remove_zeroes
                    if maths.isnan(payment_variables.IsNegAmortPayment) and maths.isnan(payment_variables.NegAmortAmount) and maths.isnan(payment_variables.IsPrepayment) and maths.isnan(payment_variables.PrepaymentAmount) and maths.isnan(payment_variables.IsIOPayment) and maths.isnan(payment_variables.ChargeOffAmount) == False:
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

                    if maths.isnan(misc_variables.BankruptcyChapterid) and maths.isnan(misc_variables.InLossMitigation) and maths.isnan(misc_variables.CurrentCreditScore) and maths.isnan(misc_variables.RemainingTerm) == False:
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

    test = cursor.execute("SELECT * FROM FullVariables").fetchall()
    test = pd.DataFrame(test).to_numpy()
    print(test)

    conn.commit()
    conn.close()
