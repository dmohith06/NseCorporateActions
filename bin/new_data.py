#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import pandasql
import re
import os
import config_reader
import logging
import platform
import os.path
import pathlib
import datetime as dt
from datetime import date
from datetime import datetime, timedelta



def create_logger():
    logger = logging.getLogger(__name__)
    if not os.path.exists('../logs'):
        os.makedirs('../logs')
    dt_str = str(dt.datetime.now()).replace(' ', '_' ).replace('-','').replace(':', '').split('.')[0]
    logging.basicConfig(filename='../logs/corporate_actions'+ dt_str+'.log', filemode='a', format='%(process)d  %(asctime)s %(levelname)s %(funcName)s %(lineno)d ::: %(message)s', level=logging.INFO)
    return logger

# importing from config.properties file
def config_imports(logger):
    try:
        config = config_reader.get_config()
        return config
    except Exception as e:
        logger.exception('ERROR:: Some issue in reading the Config...check config_reader.py script in bin Folder....')
        raise e



data = pd.read_csv(r"../inputs/CA_LAST_24_MONTHS.csv")
input_csv=pd.read_csv("../inputs/CA_LAST_24_MONTHS.csv")
converting_lower=input_csv['Purpose'].str.lower()
purpose=data['Purpose']


def Convert():
    Convert_it_tolist=list(purpose)
    Convert_it_tolist=[row.lower().strip() for row in Convert_it_tolist]
    return Convert_it_tolist


def remove_unwanted():
    After_convert=Convert()
    clear_list=[]
    for text in After_convert:
        if re.search(r'\w+',text):
            for ch in [' re',' rs','-'," ",'rs.']:
                text=text.replace(ch,'').strip()
            clear_list.append(text)
    return clear_list


def bonus():
    bonus_data=remove_unwanted()
    new_bonus_data=[]
    number=[]
    for i in bonus_data:
        data=re.findall('bonus\d*\d.?\d*',i)
        new_bonus_data.append(data)
    bonus_num=[]
    for i in new_bonus_data:
        nums=re.findall('\d*\.?\d+',str(i))
        numeric=list(map(float, nums))
        bonus_num.append(numeric)
    for i in bonus_num:
        if(len(i)==2):
            number.append((i[0]/i[1])/i[1])
        else:
            number.append(1)
    return number


def special_dividend():
    data_special_dividend=[]
    special_dividend=remove_unwanted()
    for i in special_dividend:
        data=(re.findall('specialdividend\d*\.?\d+',i) or re.findall('spldiv.\d*\.?\d+',i) or re.findall('splecialdividend\d*\.?\d+',i))
        data_special_dividend.append(data)
    special_dividend_num=[]
    for i in data_special_dividend:
        nums=re.findall('\d*\.?\d+',str(i))
        numeric=list(map(float, nums))
        special_dividend_num.append(numeric)
    number=[]
    for i in special_dividend_num:
        if(len(i)>0):
            number.append(i[0])
        else:
            number.append(1)
    return number


def interimdiv():
    data_interm_dividend=[]
    interm_dividend=remove_unwanted()
    for i in interm_dividend:
        data=(re.findall('interimdividend\d*\.?\d+',i) or re.findall('specialinterimdividend\d*\.?\d+',i) or re.findall('inerimdividend\d*\.?\d+',i) or re.findall('intdiv\d*\.?\d+',i) or re.findall('interimdiv\d*\.?\d+',i) or re.findall('interimdividendre\d*\.?\d+',i) or re.findall('intermdividend\d*\.?\d+',i) or re.findall('intdividend\d*\.?\d+',i))
        data_interm_dividend.append(data)
    interm_dividend_num=[]
    for i in data_interm_dividend:
        nums=re.findall('\d*\.?\d+',str(i))
        numeric=list(map(float, nums))
        interm_dividend_num.append(numeric)
    number=[]
    for i in interm_dividend_num:
        if(len(i)>0):
            number.append(i[0])
        else:
            number.append(1)
    return number

def dividend():
    data_dividend=[]
    dividend=remove_unwanted()
    for i in dividend:
        data=(re.findall('/dividend\d*\.?\d+',i) or (re.findall(r'^dividend\d*\.?\d+',i)))
        data_dividend.append(data)
    dividend_num=[]
    for i in data_dividend:
        nums=re.findall('\d*\.?\d+',str(i))
        numeric=list(map(float, nums))
        dividend_num.append(numeric)
    number=[]
    for i in dividend_num:
        if(len(i)>0):
            number.append(i[0])
        else:
            number.append(1)
    return number

def final_dividend():    
    final_dividend_data=[]
    final_dividend=remove_unwanted()
    for i in final_dividend:
        data=re.findall('/finaldividend\d*\.?\d+',i)
        final_dividend_data.append(data)
    final_dividend_num=[]
    for i in final_dividend_data:
        nums=re.findall('\d*\d.?\d+',str(i))
        numeric=list(map(float, nums))
        final_dividend_num.append(numeric)
    number=[]
    for i in final_dividend_num:
        if(len(i)>0):
            number.append(i[0])
        else:
            number.append(1)
    return number

def rights():    
    rights_data=[]
    rights=remove_unwanted()
    for i in rights:
        data=(re.findall('rights\d*\d.?\d*',i) or re.findall('rights:\d*\d.?\d*\w+',i))
        rights_data.append(data)
    rights_num=[]
    for i in rights_data:
        nums=re.findall('\d*\.?\d+',str(i))
        numeric=list(map(float, nums))
        rights_num.append(numeric)
    number=[]
    for i in rights_num:
        if(len(i)==2):
            number.append(i[0]/(i[0]+i[1]))
        else:
            number.append(1)
    return number
    
def premium():    
    premium_data=[]
    premium=remove_unwanted()
    for i in premium:
        data=(re.findall('premium\d*\.?\d+',i) or re.findall('premiumof\d*\.?\d+',i))
        premium_data.append(data)
    premium_num=[]
    for i in premium_data:
        nums=re.findall('\d*\.?\d+',str(i))
        numeric=list(map(float, nums))
        premium_num.append(numeric)
    number=[]
    for i in premium_num:
        if(len(i)>0):
            number.append(i[0])
        else:
            number.append(1)
    return number
    
def consolidation():
    consolidation_data=[]
    consolidation=remove_unwanted()
    for i in consolidation:
        data=re.findall('consolidation\d*\.?\d+\w+\d*\.?\d+',i)
        consolidation_data.append(data)
    consolidation_num=[]
    for i in consolidation_data:
        nums=re.findall('\d*\.?\d+\d*\.?\d+',str(i).replace('to.',','))
        numeric=list(map(float, nums))
        consolidation_num.append(numeric)
    number=[]
    for i in consolidation_num:
        if(len(i)>0):
            number.append(i[0])
        else:
            number.append(1)
    return number

def facevaluediv():
    facevalue_data=[]
    facevalue=remove_unwanted()
    for i in facevalue:
        data=re.findall(r'from\d*\d.?\d*\w+',i)
        facevalue_data.append(data)
    facevalue_num=[]
    for i in facevalue_data:
        nums=re.findall('\d*\.?\d+',str(i))
        numeric=list(map(float, nums))
        facevalue_num.append(numeric)
    number=[]
    for i in facevalue_num:
        if(len(i)==2):
            number.append(i[1]/i[0])
        else:
            number.append(1)
    return number    

def distribution():    
    distribution_data=[]
    distribution=remove_unwanted()
    for i in distribution:
        data=re.findall('^distribution\d*\.?\d+',i)
        distribution_data.append(data)
    distribution_num=[]
    for i in distribution_data:
        nums=re.findall('\d*\.?\d+',str(i))
        numeric=list(map(float, nums))
        distribution_num.append(numeric)
    number=[]
    for i in distribution_num:
        if(len(i)>0):
            number.append(i[0])
        else:
            number.append(1)
    return number

def fourth_Distribution():    
    fourth_Distribution_data=[]
    fourth_Distribution=remove_unwanted()
    for i in fourth_Distribution:
        data=re.findall('^fourthdistribution\d*\.?\d+',i)
        fourth_Distribution_data.append(data)
    fourth_Distribution_data_num=[]
    for i in fourth_Distribution_data:
        nums=re.findall('\d*\.?\d+',str(i))
        numeric=list(map(float, nums))
        fourth_Distribution_data_num.append(numeric)
    number=[]
    for i in fourth_Distribution_data_num:
        if(len(i)>0):
            number.append(i[0])
        else:
            number.append(1)
    return number

def interest_payment():    
    interest_payment_data=[]
    interest_payment=remove_unwanted()
    for i in interest_payment:
        data=re.findall('interestpayment\d*\.?\d+',i)
        interest_payment_data.append(data)
    interest_payment_data_num=[]
    for i in interest_payment_data:
        nums=re.findall('\d*\.?\d+',str(i))
        numeric=list(map(float, nums))
        interest_payment_data_num.append(numeric)
    number=[]
    for i in interest_payment_data_num:
        if(len(i)>0):
            number.append(i[0])
        else:
            number.append(1)
    return number


def return_of_capital():    
    return_of_capital_data=[]
    return_of_capital=remove_unwanted()
    for i in return_of_capital:
        data=(re.findall('turnofcapitalre\d*\d.?\d*',i) or re.findall('turnofcapital\d*\d.?\d*',i))
        return_of_capital_data.append(data)
    return_of_capital_data_num=[]
    for i in return_of_capital_data:
        nums=re.findall('\d*\.?\d+\d*\.?\d+',str(i).replace('to',','))
        numeric=list(map(float, nums))
        return_of_capital_data_num.append(numeric)
    number=[]
    for i in return_of_capital_data_num:
        if(len(i)>0):
            number.append(i[0])
        else:
            number.append(1)
    return number

def capital_reduction():    
    capital_reduction_data=[]
    capital_reduction=remove_unwanted()
    for i in capital_reduction:
        data=re.findall('capitalduction\d*\.?\d+\w+\d*\.?\d+',i)
        capital_reduction_data.append(data)
    capital_reduction_data_num=[]
    for i in capital_reduction_data:
        nums=re.findall('\d*\.?\d+\d*\.?\d+',str(i).replace('to',','))
        numeric=list(map(float, nums))
        capital_reduction_data_num.append(numeric)
    number=[]
    for i in capital_reduction_data_num:
        if(len(i)==2):
            result=np.round((i[0]/i[1]),2)
            number.append(result)
        else:
            number.append(1)
    return number




def main():
    logger = create_logger()
    config = config_imports(logger)
    logger.info('Config == %s', config)
    facevalue=facevaluediv()
    input_csv['Facevaluesplit']=pd.DataFrame(facevalue).astype(float)
    premium1=premium()
    input_csv['Premium']=pd.DataFrame(premium1).astype(float)
    interim=interimdiv()
    input_csv['InterimDividend']=pd.DataFrame(interim).astype(float)
    bonus1=bonus()
    input_csv['Bonus']=pd.DataFrame(bonus1).astype(float)
    special_dividend1=special_dividend()
    input_csv['SpecialDividend']=pd.DataFrame(special_dividend1).astype(float)
    dividend1=dividend()
    input_csv['Dividend']=pd.DataFrame(dividend1).astype(float)
    final_dividend1=final_dividend()
    input_csv['FinalDividend']=pd.DataFrame(final_dividend1).astype(float)
    rights1=rights()
    input_csv['Rights']=pd.DataFrame(rights1).astype(float)
    consolidation1=consolidation()
    input_csv['Consolidation']=pd.DataFrame(consolidation1).astype(float)
    distribution1=distribution()
    input_csv['Distribution']=pd.DataFrame(distribution1).astype(float)
    fourth_Distribution1=fourth_Distribution()
    input_csv['FourthDistribution']=pd.DataFrame(fourth_Distribution1).astype(float)
    interest_payment1=interest_payment()
    input_csv['InterestPayment']=pd.DataFrame(interest_payment1).astype(float)
    return_of_capital1=return_of_capital()
    input_csv['ReturnofCapital']=pd.DataFrame(return_of_capital1).astype(float)
    capital_reduction1=capital_reduction()
    input_csv['CapitalReduction']=pd.DataFrame(capital_reduction1).astype(float)
    input_csv.to_csv("../corporate_data/corporate_actions.csv",index=False)
    return input_csv




















