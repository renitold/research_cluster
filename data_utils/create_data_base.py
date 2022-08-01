#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 17:13:43 2022

@author: danieldomek
"""
import numpy as np
import pandas as pd
import os
import requests as r
from tqdm import tqdm
import datetime

if __name__ == "__main__":
    
    path = r'/Users/danieldomek/downloads'
    stuff = os.listdir(r'/Users/danieldomek/downloads')
    
    symbol = 'QQQ'
    f = open(path+'/'+symbol+'.txt')
    txt = f.read()
    f.close()
    
    print('loaded')
    txt = txt.split('\n')
    txt = [x.split(',') for x in txt]
    txt.pop(-1)    
    print('formatted')
    #4am to 8pm est
    all_dates = list()
    for x in range(0,len(txt)):
        all_dates.append(txt[x][0])
        
    all_dates = np.unique(all_dates).astype(str).tolist()
    for x in tqdm(range(0,len(all_dates))):
        curr = all_dates[x]
        curr = datetime.datetime.strptime(curr,"%m/%d/%Y")
        all_dates[x] = curr
        
    partitioned = {}
    for x in range(0,len(all_dates)):
        partitioned[all_dates[x].strftime("%m/%d/%Y")] = []
        
    for x in tqdm(range(0,len(txt)),desc="partition"):
        partitioned[txt[x][0]].append(txt[x][1:])
        
    all_times = list()
    for x in range(4,20):
        for y in range(0,60):
            hour = str(x)
            minute = str(y)
            if(len(hour)==1):
                hour = '0'+hour
            if(len(minute)==1):
                minute = '0'+minute
            all_times.append(hour+':'+minute)
        
    time_map = {}
    for t in all_times:
        time_map[t] = []
        
    for i,j in partitioned.items():
        for y in range(len(j)-1,-1,-1):
            hour = int(j[y][0][:2])
            if(hour<4 or hour>19):
                j.pop(y)
            
        for g in j:
            time_map[g[0]] = g.copy()
        
        f_elem = -1
        for y in range(0,len(all_times)):
            if(len(time_map[all_times[y]])>0):
                f_elem  = y
                break
            
        if(f_elem>0):
            for y in range(0,f_elem):
                time_map[all_times[y]] = time_map[all_times[f_elem]].copy()
                time_map[all_times[y]][0] = all_times[y]
                time_map[all_times[y]][-1] = '0'
            for y in range(f_elem+1, len(all_times)):
                if(len(time_map[all_times[y]])==0):
                    time_map[all_times[y]] = time_map[all_times[y-1]].copy()
                    time_map[all_times[y]][-1] = '0'
                    time_map[all_times[y]][0] = all_times[y]
                    
        else:
            for y in range(0+1, len(all_times)):
                if(len(time_map[all_times[y]])==0):
                    time_map[all_times[y]] = time_map[all_times[y-1]].copy()
                    time_map[all_times[y]][-1] = '0'
                    
        new = []
        for y in range(0,len(all_times)):
            adj = time_map[all_times[y]]
            adj.insert(0,symbol)
            new.append(time_map[all_times[y]])
            time_map[all_times[y]] = []
            
        partitioned[i] = new
        
        
        
                
        
        
    
            
        
    txt = ''
    
    save_path = r'/Users/research_cluster/stocks_all_history'
    
    keys = list(partitioned.keys())
    for x in tqdm(range(0,len(keys)),desc='saving files'):
        strr = ""
        j = partitioned[keys[x]]
        for y in range(0,len(j)):
            for g in j[y]:
                strr+=g+','
            strr = strr[:-1]
            if(y!=len(j)-1):
                strr+='\n'
        
        keys[x] = keys[x].split('/')
        keys[x] = keys[x][-1]+'-'+keys[x][0]+'-'+keys[x][1]
        f = open(save_path+'/'+keys[x]+'.txt', 'a')
        f.write('\n'+strr)
        f.close()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    