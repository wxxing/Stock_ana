from sqlalchemy import create_engine
import pandas as pd
from pandas import Series,DataFrame
import numpy as np
from datetime import datetime,timedelta
import sqlalchemy

engine = create_engine('mysql+pymysql://root:root@127.0.0.1/tushare?charset=utf8')
df = pd.read_sql('select * from sh_000123', engine)
df['datetime'] = df['日期']+' '+df['时间']
df['datetime'] = pd.to_datetime(df['datetime'],format='%Y-%m-%d %H:%M:%S')
df=df.set_index('datetime')

day = '2016-1-14'
d = datetime.strptime(day,'%Y-%m-%d')

A = [[0,0],[1,1],[2,2],[4,4],[1,0],[2,0],[4,0],[30,0],[60,0]]
B = [['00:00','23:59','全天'],['00:00','12:00','上午'],['12:00','23:59','下午'],['9:30','10:30','9.5到10.5'],['10:30','11:30','10.5到11.5'],['13:00','14:00','13点到14点'],['14:00','15:00','14点到15点']]  # 时间段
amount = 40000  #大单>=amount


def main():
    l = ['大单次数', '大单量占总比', '大单中S次数', '大单中B次数', 'S/B', '大单最早时间', '大单最晚时间', '平均时间','大单均价/总均价']
    final_df = DataFrame(index=l)
    final_df.index.name='index'
    for x in A:     #遍历日期
        d1 = d - timedelta(x[0])
        d2 = d - timedelta(x[1])
        df1 = df['%s'%(d1.date()):'%s'%(d2.date())]  #日期筛选-总表
        df2 = df1[df1['成交量']>=amount]  #日期筛选-大单
        if df2.size == 0:
            pass
        else:
            earliest = str(df2.index.values[0])[:19]
            earliest = earliest.replace('T', ' ')
            lastest = str(df2.index.values[-1])[:19]
            lastest = lastest.replace('T', ' ')
            mean_time = str(df2.index.values[0])[:19]
            mean_time = mean_time.replace('T', ' ')

            for y in B:     #遍历时间段
                df3=df1.between_time(y[0],y[1])  #日期筛选-总表
                df4=df3[df3['成交量']>=amount]  #日期筛选-大单
                if df4.size == 0:
                    pass
                else:
                    dadan_times = len(df4)  #大单次数
                    dadan_rate = str(df4['成交量'].sum()/df3['成交量'].sum())   #大单量占从成交量比例
                    s_times = len(df4[df4['BS']=='S'])  #s次数
                    b_times = len(df4[df4['BS']=='B'])  #b次数
                    if b_times == 0:
                        sdivb = 0
                    else:
                        sdivb = s_times/b_times
                    #mean_price = '%.4f'%(df3['成交价'].mean()/df2['成交价'].mean())
                    mean_price = str(df4['成交价'].mean()/df3['成交价'].mean())

                    l1 = [dadan_times, dadan_rate, s_times, b_times, sdivb, earliest, lastest, mean_time, mean_price]
                    ser = Series(l1,index=l)
                    final_df['%s月%s日至%s月%s日/%s'%(d1.month,d1.day,d2.month,d2.day,y[2])] = ser
    #final_df = final_df.T
    #final_df.index.name='index'
    final_df.to_sql('sh123456',engine,if_exists='replace',dtype={'index': sqlalchemy.VARCHAR(20)})

if __name__ == '__main__':
    main()