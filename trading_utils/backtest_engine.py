#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 30 08:32:44 2022

@author: danieldomek
"""
import numpy as np
import pandas as pd
import os
import requests as r
from tqdm import tqdm
import datetime
import time
import json
from matplotlib import pyplot as plt

class stock_data():
    def __init__(self,stocks,dates,days,times):
        self.pathh = r'/Users/research_cluster/stocks_all_history/'
        self.mapp_pathh = r'/Users/research_cluster/stocks_all_history_maps/'
        self.stocks = stocks
        self.dates = dates
        self.days = days
        self.times = times
        self.dirs = os.listdir(pathh)
        self.dirs = sorted(dirs)[1:]
        self.full_range = []
        self.raw_data = dict()
        self.structured_data = {}
        self.curr_ind = self.days
        self.inBounds = True
        self.curr_data = {}
        
        for x in self.stocks:
            self.raw_data[x] = []
            self.structured_data[x] = []
            self.curr_data[x] = []
            
        self.roll_date_range()
        self.begin_rolling()
        self.prog_bar = tqdm(total=100)
        
        self.ydate = None
        self.tdate = None
        
    def roll_date_range(self):
        for x in range(len(self.dirs)):
            if(self.dirs[x]>=self.dates[0] and self.dirs[x]<=self.dates[1]):
                if(len(self.full_range)==0):
                    for y in range(x-9,x):
                        self.full_range.append(self.dirs[y])
                self.full_range.append(self.dirs[x])
        
    def begin_rolling(self):
        for x in tqdm(range(0,self.curr_ind), desc="initial data roll"):
            f = open(self.pathh+'/'+self.full_range[x])
            data = f.read()
            f.close()
            data = data.split('\n')
            data = [row.split(',') for row in data]
            f = open(self.mapp_pathh+'/'+self.full_range[x])
            mapp = f.read()
            f.close()
            mapp = json.loads(mapp)
            
            begin_hour,begin_minute = self.times[0].split(':')
            begin_hour,begin_minute = int(begin_hour),int(begin_minute)
            
            end_hour,end_minute = self.times[1].split(':')
            end_hour,end_minute = int(end_hour),int(end_minute)
            
            begin_time_diff = (begin_hour*60+begin_minute)-4*60
            end_time_diff = (end_hour*60+end_minute)-(begin_hour*60+begin_minute)
            
            for sym in self.stocks:
                self.raw_data[sym].append(data[mapp[sym]+begin_time_diff:mapp[sym]+begin_time_diff+end_time_diff])
                
        for sym in self.stocks:
            all_dat = self.raw_data[sym]
            for data in all_dat:
                self.structured_data[sym]+=data
                
        x = self.curr_ind
        f = open(self.pathh+'/'+self.full_range[x])
        data = f.read()
        f.close()
        data = data.split('\n')
        data = [row.split(',') for row in data]
        f = open(self.mapp_pathh+'/'+self.full_range[x])
        mapp = f.read()
        f.close()
        mapp = json.loads(mapp)
        
        begin_hour,begin_minute = self.times[0].split(':')
        begin_hour,begin_minute = int(begin_hour),int(begin_minute)
        
        end_hour,end_minute = self.times[1].split(':')
        end_hour,end_minute = int(end_hour),int(end_minute)
        
        begin_time_diff = (begin_hour*60+begin_minute)-4*60
        end_time_diff = (end_hour*60+end_minute)-(begin_hour*60+begin_minute)
        
        for sym in self.stocks:
            self.curr_data[sym] = data[mapp[sym]+begin_time_diff:mapp[sym]+begin_time_diff+end_time_diff]
        
        self.ydate = self.full_range[self.curr_ind-1][:10]
        self.tdate = self.full_range[self.curr_ind][:10]
            
                
                
    def roll_fwd(self):
        if(self.curr_ind==len(self.full_range)):
            self.inBounds = False
            self.prog_bar.close()
            return()
        div = np.ceil(len(self.full_range)/100)
        if(self.curr_ind%div==0):
            self.report_progress()
        x = self.curr_ind
        f = open(self.pathh+'/'+self.full_range[x])
        data = f.read()
        f.close()
        data = data.split('\n')
        data = [row.split(',') for row in data]
        f = open(self.mapp_pathh+'/'+self.full_range[x])
        mapp = f.read()
        f.close()
        mapp = json.loads(mapp)
        
        begin_hour,begin_minute = self.times[0].split(':')
        begin_hour,begin_minute = int(begin_hour),int(begin_minute)
        
        end_hour,end_minute = self.times[1].split(':')
        end_hour,end_minute = int(end_hour),int(end_minute)
        
        begin_time_diff = (begin_hour*60+begin_minute)-4*60
        end_time_diff = (end_hour*60+end_minute)-(begin_hour*60+begin_minute)
        
        for sym in self.stocks:
            self.raw_data[sym].pop(0)
            self.raw_data[sym].append(data[mapp[sym]+begin_time_diff:mapp[sym]+begin_time_diff+end_time_diff])
            
        for sym in self.stocks:
            all_dat = self.raw_data[sym]
            self.structured_data[sym] = []
            for data in all_dat:
                self.structured_data[sym][0:0]=data
                
                
        self.curr_ind+=1
        if(self.curr_ind==len(self.full_range)):
            self.inBounds = False
            self.prog_bar.close()
            return()
        
        x = self.curr_ind
        f = open(self.pathh+'/'+self.full_range[x])
        data = f.read()
        f.close()
        data = data.split('\n')
        data = [row.split(',') for row in data]
        f = open(self.mapp_pathh+'/'+self.full_range[x])
        mapp = f.read()
        f.close()
        mapp = json.loads(mapp)
        
        begin_hour,begin_minute = self.times[0].split(':')
        begin_hour,begin_minute = int(begin_hour),int(begin_minute)
        
        end_hour,end_minute = self.times[1].split(':')
        end_hour,end_minute = int(end_hour),int(end_minute)
        
        begin_time_diff = (begin_hour*60+begin_minute)-4*60
        end_time_diff = (end_hour*60+end_minute)-(begin_hour*60+begin_minute)
        
        for sym in self.stocks:
            self.curr_data[sym] = data[mapp[sym]+begin_time_diff:mapp[sym]+begin_time_diff+end_time_diff]
        
        self.ydate = self.full_range[self.curr_ind-1][:10]
        self.tdate = self.full_range[self.curr_ind][:10]
        
        
        
                
    def get_data_df(self,symbol,n_days):
        if(n_days>self.days):
            raise "n_days is greater than full lookback"
        data = list()
        for x in range(len(self.raw_data[symbol])-n_days,len(self.raw_data[symbol])):
            data+=self.raw_data[symbol][x]
            
        data = pd.DataFrame(data,columns=['Symbol','Time','Open','High','Low','Close','Volume'])
        data = data.astype({'Symbol':str,'Time':str,'Open':float,'High':float,'Low':float,'Close':float,'Volume':int})   
        return(data)
    
    def report_progress(self):
        self.prog_bar.update(1)
        
    
        
        

        
class position_manager():
    def __init__(self):
        self.positions = {}
        self.pnl = []
        self.pnl_bps = []
        self.trades = []
        self.inPos = False
        self.curr_position = {
            "symbol":'',
            "side":'',
            "entry_date":'',
            "entry_time":'',
            "exit_date":'',
            "exit_time":'',
            "entry_price":'',
            "exit_price":'',
            "stop_loss":'',
            "target_px":'',
            "size":''
            }
    def open_new_position(self, symbol, entry_date,entry_time, entry_price, size, side):
        if(self.positions.get(symbol,None)!=None):
            raise "There is already a position open"
        else:
            curr_position = self.curr_position.copy()
            curr_position['symbol'] = symbol
            curr_position['side'] = side
            curr_position['entry_date'] = entry_date
            curr_position['entry_time'] = entry_time
            curr_position['entry_price'] = entry_price
            curr_position['size'] = size
            
            self.positions[symbol] = curr_position
            self.inPos = True
    
    def close_old_position(self,symbol, exit_date,exit_time,exit_price):
        if(self.positions.get(symbol,None)==None):
            raise "you don't have any open positions for this symbol"
        else:
            self.positions[symbol]['exit_date'] = exit_date
            self.positions[symbol]['exit_time'] = exit_time
            self.positions[symbol]['exit_price'] = exit_price
            
            self.trades.append(self.positions[symbol])
            if(self.positions[symbol]['side']=="LONG"):
                self.pnl.append((self.positions[symbol]['exit_price']-self.positions[symbol]['entry_price'])*self.positions[symbol]['size'])
                self.pnl_bps.append((self.positions[symbol]['exit_price']/self.positions[symbol]['entry_price'])-1)
            elif(self.positions[symbol]['side']=="SHORT"):
                self.pnl.append((self.positions[symbol]['entry_price']-self.positions[symbol]['exit_price'])*self.positions[symbol]['size'])
                self.pnl_bps.append((self.positions[symbol]['entry_price']-self.positions[symbol]['exit_price'])/self.positions[symbol]['entry_price'])
            else:
                side = self.positions[symbol]['side']
                raise f'not valid position expected LONG or SHORT got: {side}'
                
            del self.positions[symbol]
            
            if(len(list(self.positions.keys()))==0):
                self.inPos = False
            
    def check_in_pos(self):
        if(len(list(self.positions.keys()))==0):
            return(False)
        else:
            return(True)
            
    def plot_eq_curve(self):
        plt.plot(np.cumsum(self.pnl_bps))
        plt.show()
        
        
        


if __name__ == "__main__":
    pathh = r'/Users/research_cluster/stocks_all_history/'
    dirs = os.listdir(pathh)
    dirs = sorted(dirs)[1:]
    
    stocks = ['SPY']*500
    dates = ['2020-01-01','2020-12-31']
    times = ['04:00','20:00']
    days = 5
    sd = stock_data(stocks,dates,days, times)
    pm = position_manager()
    
    corrs = list()
    while(sd.inBounds):
        df_qqq = sd.get_data_df(stocks[1],5)
        df_spy = sd.get_data_df(stocks[0],5)
        
        correl = np.corrcoef(np.diff(df_qqq.Close)[1:], np.diff(df_spy.Close)[1:])
        corrs.append(correl[0][1])
        if(correl[0][1]>.1):
            q_range = (df_qqq.Close.iloc[-1]-df_qqq.Low.min())/(df_qqq.High.max()-df_qqq.Low.min())
            s_range = (df_spy.Close.iloc[-1]-df_spy.Low.min())/(df_spy.High.max()-df_spy.Low.min())
            if(q_range-s_range>.3 and not pm.inPos):
                pm.open_new_position("QQQ", sd.ydate, '15:59', df_qqq.Close.iloc[-1], 1, "LONG")
                print('opening position')
        if(pm.inPos):
            if((q_range-s_range)<.1):
                pm.close_old_position("QQQ", sd.ydate, "15:59", df_qqq.Close.iloc[-1])
                print('CLOSING POSITION')
                
        sd.roll_fwd()
        
    pm.plot_eq_curve()
    
    # f = open(r'/Users/research_cluster/stocks_all_history_maps/'+'2021-01-04.txt')
    # one_map = f.read()
    # f.close()
    
    # json.loads(one_map)
    
    
    
    
    
    
    
    