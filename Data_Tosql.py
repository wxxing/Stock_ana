import os
from sqlalchemy import create_engine

import pymysql
import pandas as pd
import numpy as np
import threading
from multiprocessing import Process
import time


def div_list(ls,n):
    #
    if not isinstance(ls,list) or not isinstance(n,int):
        return []
    ls_len = len(ls)
    if n<=0 or 0==ls_len:
        return []
    if n > ls_len:
        return []
    elif n == ls_len:
        return [[i] for i in ls]
    else:
        j = ls_len//n
        k = ls_len%n
        ### j,j,j,...(前面有n-1个j),j+k
        #步长j,次数n-1
        ls_return = []
        for i in range(0,(n)*j,j):
            ls_return.append(ls[i:i+j])
        #算上末尾的j+k
        ls_return.append(ls[(n)*j:])
        return ls_return

def getListFiles(path):
    ret = []
    for root, dirs, files in os.walk(path):
        for filespath in files:
            ret.append(os.path.join(root,filespath))
    return ret




def main(ls, thread):
    engine = create_engine('mysql+pymysql://root:root@127.0.0.1/tushare?charset=utf8')

    for each in ret:

        if len(each) > 19:
            if each.split('.')[-1] == 'csv' and each[-19] in ["0","6","3"]:


                if each[-19:-13] in ls:
                    if 'SZ_%s' % each[-19:-13] in l:   # 判断此表是否已经存在
                        Alldate = pd.read_sql('select distinct 日期 from SZ_%s' % each[-19:-13], engine)  # 已存在则读出已经保存的所有日期
                        if '%s-%s-%s'%(each[-12:-8],int(each[-8:-6]),int(each[-6:-4])) in Alldate['日期'].values:   # 判断将要保存的日期是否已经存在
                            print('正在执行%s:表SZ_%s的%s-%s-%s日数据已存在!' % (thread, each[-19:-13], each[-12:-8],int(each[-8:-6]),int(each[-6:-4])))  # 如果存在则pass
                        else:   # 如果不存在则保存
                            df = pd.read_csv(each, encoding='gbk')  # filename可以直接从盘符开始，标明每一级的文件夹直到csv文件，header=None表示头部为空，sep=' '表示数据间使用空格作为分隔符，如果分隔符是逗号，只需换成 ‘，’即可。
                            df.to_sql('SZ_%s' % each[-19:-13], engine, if_exists='append')
                            print('正在执行%s:表SZ_%s已存在,正在保存%s-%s-%s日数据...' % (thread, each[-19:-13], each[-12:-8],int(each[-8:-6]),int(each[-6:-4])))

                    else:   # 不存在此表则直接保存
                        df = pd.read_csv(each,encoding='gbk')  # filename可以直接从盘符开始，标明每一级的文件夹直到csv文件，header=None表示头部为空，sep=' '表示数据间使用空格作为分隔符，如果分隔符是逗号，只需换成 ‘，’即可。
                        df.to_sql('SZ_%s' % each[-19:-13], engine, if_exists='append')
                        print('正在执行%s:表SZ_%s不存在,正在保存%s-%s-%s日数据...' % (thread, each[-19:-13], each[-12:-8],int(each[-8:-6]),int(each[-6:-4])))


if __name__ == '__main__':
    start = time.time()
    # 连接数据库并读出所有表名
    conn = pymysql.connect(host="localhost", user="root", passwd="root", db="tushare", charset="utf8")
    cursor = conn.cursor()
    cursor.execute("show tables")
    l = []
    for i in cursor.fetchall():
        l.append(str(i[0]))

    p = '/home/wu/201605sh股票五档分笔/'
    allpath = os.listdir(p)
    for path in allpath:
        ret = getListFiles(p+path)
        allcode = []

        for each in ret:

            if len(each) > 19:
                if each.split('.')[-1] == 'csv' and each[-19] in ["0","6","3"]:
                    allcode.append(each[-19:-13])


        dl = div_list(allcode, 20)

        pool = []
        for i in range(21):
            t = Process(target=main, args=(dl[i], '进程%s'%i))
            pool.append(t)


        for t in pool:
            t.start()

        for t in pool:
            t.join()




    cursor.close()
    conn.close()

    end = time.time()
    print('程序运行时间:%s'%(end-start))
pass
