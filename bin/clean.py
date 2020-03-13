#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
import pandasql
import re
import new_data


query1="""SELECT a.symbol,a.company,a.series,a.facevalue,a.purpose,a.exdate,a.specialdividend,a.interimdividend,a.premium,a.dividend,a.bonus,a.finaldividend,a.rights,a.facevaluesplit,a.distribution,a.fourthdistribution,a.interestpayment,a.returnofcapital,a.capitalreduction,a.consolidation,(SELECT close FROM nse_daily WHERE symbol=a.symbol AND series=a.series AND timestamp=a.exdate) AS ext_price,c.prevclose as cum_price,c.timestamp as cum_date from input_corporate a inner join nse_daily c on a.symbol=c.symbol AND a.series=c.series AND c.timestamp=(SELECT MAX(timestamp) FROM nse_daily WHERE symbol=a.symbol AND series=a.series AND timestamp<a.exdate)"""
query2="""SELECT s.symbol,s.company,s.series,s.purpose,s.cum_date,data.open,data.close,data.prevclose,data.high,data.low,data.last,data.timestamp,s.factors*data.factors as factorMul from sub_data s inner join input_csv data where s.series=data.series and s.symbol=data.symbol and data.timestamp<s.exdate"""

def wanted_columns_from_csv1():
    df=new_data.main()
    df.columns=map(str.lower,df.columns)
    wanted_columns_from_dataframe=df[['symbol','company','series','face value(rs.)','purpose','ex-date','record date','premium','bonus','interimdividend','specialdividend','dividend','finaldividend','rights','facevaluesplit','consolidation','distribution','fourthdistribution','interestpayment','returnofcapital','capitalreduction']]
    wanted_columns_from_dataframe['ex-date'] = pd.to_datetime(wanted_columns_from_dataframe['ex-date']).dt.date
    wanted_columns_from_dataframe = wanted_columns_from_dataframe.rename(columns={'ex-date':'exdate','face value(rs.)':'facevalue','record date':'recorddate'})
    wanted_columns_from_dataframe.index = np.arange(1, len(wanted_columns_from_dataframe)+1)
    return wanted_columns_from_dataframe


def wanted_columns_from_csv2():
    input_csv2=pd.read_csv("../inputs/2018_20.csv")
    input_csv2.columns=map(str.lower,input_csv2.columns)
    input_csv2['timestamp']= pd.to_datetime(input_csv2['timestamp']).dt.date
    wanted_columns_from_dataframe1=input_csv2[['symbol','series','open','high','low','close','prevclose','timestamp']]
    wanted_columns_from_dataframe1.index = np.arange(1, len(wanted_columns_from_dataframe1)+1)
    return wanted_columns_from_dataframe1

def subdata1():
    input_corporate=wanted_columns_from_csv1()
    nse_daily=wanted_columns_from_csv2()
    return pandasql.sqldf(query1, locals())
    
def data_from_sql1():
    sub_data=subdata1()
    sub_data.index = np.arange(1, len(sub_data)+1)
    sub_data['interimdividend']=round((sub_data['cum_price']-sub_data['interimdividend'])/(sub_data['cum_price']),3)
    sub_data['finaldividend']=round((sub_data['cum_price']-sub_data['finaldividend'])/(sub_data['cum_price']),3)
    sub_data['dividend']=round((sub_data['cum_price']-sub_data['dividend'])/(sub_data['cum_price']),3)
    sub_data['fourthdistribution']=round((sub_data['cum_price']-sub_data['fourthdistribution'])/(sub_data['cum_price']),3)
    sub_data['distribution']=round((sub_data['cum_price']-sub_data['distribution'])/(sub_data['cum_price']),3)
    rights_calc=((sub_data['cum_price']+(sub_data['premium']*sub_data['rights']))/(1+sub_data['rights']))
    sub_data['rights']=rights_calc/sub_data['cum_price']
    sub_data['rights']=round(sub_data['rights'],3)
    sub_data['returnofcapital']=round((sub_data['cum_price']-sub_data['returnofcapital'])/(sub_data['cum_price']),3)
    sub_data['interestpayment']=round((sub_data['cum_price']-sub_data['interestpayment'])/(sub_data['cum_price']),3)
    sub_data['factors']=round(((sub_data['interimdividend']*sub_data['dividend']*sub_data['finaldividend']*sub_data['returnofcapital']*sub_data['fourthdistribution']*sub_data['distribution']*sub_data['interestpayment']*sub_data['rights']*sub_data['consolidation'])/(sub_data['facevaluesplit']*sub_data['bonus']*sub_data['capitalreduction'])),3)
    return sub_data


def csvdata():
    input_csv2=pd.read_csv("../inputs/2018_20.csv")
    input_csv2.columns=map(str.lower,input_csv2.columns)
    input_csv2['timestamp']= pd.to_datetime(input_csv2['timestamp']).dt.date
    input_csv2['factors']=1
    return input_csv2

def factmul():
    input_csv=csvdata()
    sub_data=data_from_sql1()
    return pandasql.sqldf(query2, locals())
    

def function():
    adjustment_function=factmul()
    adjustment_function['lastAdj']=adjustment_function['last']*adjustment_function['factorMul']
    adjustment_function['lowAdj']=adjustment_function['low']*adjustment_function['factorMul']
    adjustment_function['highAdj']=adjustment_function['high']*adjustment_function['factorMul']
    adjustment_function['openAdj']=adjustment_function['open']*adjustment_function['factorMul']
    adjustment_function['closeAdj']=(adjustment_function['close'].astype(float))*(adjustment_function['factorMul'].astype(float))
    adjustment_function.to_csv("../cleaned_data/cleandata.csv",index=False)
    return adjustment_function


if __name__=="__main__":
    function()
    
    