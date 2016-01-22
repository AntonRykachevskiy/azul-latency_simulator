# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 19:16:05 2016

@author: anton
"""
from enum import Enum
from random import normalvariate, lognormvariate
from SortedCollection import SortedCollection
import matplotlib.pyplot as plt 
from math import log


class EventType(Enum):
    new_request = 1
    end_request = 2
    kernel_release = 3
    kernel_occupy = 4
   
    
class Request:
    def __init__ (self, request_type = None, start_time = None, request_num = None):
        self.__request_type = request_type
        self.__start_time = start_time
        self.__request_num = request_num
        self.__end_time = None
    def setEndTime(self, end_time):
        self.__end_time = end_time
    def getRequestType(self) :
        return self.__request_type
    def getRequestStartTime(self):
        return self.__start_time
    def getRequestNumber(self):
        return self.__request_num
    def printRequest(self):
        print(self.__request_type, self.__start_time, self.__end_time, self.__request_num)    
    def getWorkTime(self):
#        if self.__end_time and self.__start_time:
        return self.__end_time - self.__start_time
            
class RequestGenerator:
    '''
    - throughput in req per sec
    - end time in sec
    - request_list in microseconds
    '''
    def __init__ (self, throughput = None, end_time = None):
        microsecond = 1000000         
        self.__request_list = []
        if throughput != None and end_time != None:
            for i in range(int(end_time * throughput)):
                request = Request("sr", float(microsecond * i / throughput), i)
                self.__request_list.append(request)        
    def getRequestList(self):
        return self.__request_list


class TimelineEvent:
    def __init__(self, event_time = None, event_type = None, request_num = None):
        self.__event_time = event_time
        self.__event_type = event_type
        self.__request_num = request_num
    def getEventTime(self):
        return self.__event_time
    def getEventType(self):
        return self.__event_type
    def getRequestNumber(self):
        return self.__request_num
    def printEvent(self):
        print(self.__event_type, self.__event_time, self.__request_num)
    

    
class RequestProcessor:
    def __init__ (self, kernels = 1, request_list = None):
        self.__timeline = SortedCollection([], TimelineEvent.getEventTime)
        self.__kernels = kernels
        self.__queue = []
        self.__free_kernels = kernels
        if request_list != None:
            for request in request_list:
                event = TimelineEvent(request.getRequestStartTime(), EventType.new_request, request.getRequestNumber())
                self.__timeline.insert(event)
        self.__computation_times = []
        
    def processNewRequest(self, request, new_request_event):
        computation_mean = 0
        computation_sigma = 10        
        computation_time = 1000 + normalvariate(computation_mean, computation_sigma)
        self.__computation_times.append(computation_time)
        if self.__free_kernels > 0:
            kernel_occupation_event = TimelineEvent(new_request_event.getEventTime(), EventType.kernel_occupy)
            self.__timeline.insert_right(kernel_occupation_event)
            self.__free_kernels -= 1
            end_time = new_request_event.getEventTime() + computation_time
            request.setEndTime(end_time)
            request_finish_event = TimelineEvent(end_time, EventType.end_request, request.getRequestNumber())
            kernel_release_event = TimelineEvent(end_time, EventType.kernel_release)
            self.__timeline.insert(request_finish_event)
            self.__timeline.insert_right(kernel_release_event)
        else:
            self.__queue.append(request)
    
    def processKernelReleaseRequest(self, kernel_release_event):
        self.__free_kernels += 1
        
        if self.__queue:
            request = self.__queue.pop(0)            
            self.processNewRequest(request, kernel_release_event)
            
    
    def processRequests(self, request_list = None):
        if request_list: 
            for event in self.__timeline:
#                event.printEvent()
#                print("\n")
#                self.printTimeline()
#                print("\n")
                if event.getEventType() == EventType.new_request:
                    self.processNewRequest(request_list[event.getRequestNumber()], event)
                if event.getEventType() == EventType.kernel_release:
                    self.processKernelReleaseRequest(event)
        
    def getTimeline(self):
        return self.__timeline
        
    def printTimeline(self):
        for event in self.__timeline:
            event.printEvent()
    
    def getComputationTimes(self):
        return self.__computation_times
              
def plotDistribution(times, min_range = None, max_range = None):
    distr = [0 for i in range(int(max(times)) + 1)]
    for t in times:
        distr[int(t)] += 1
    min_r = 0
    max_r = len(distr)    
    if max_range and min_range:
        max_r = max_range
        min_r = min_range
        
    plt.plot(distr[min_r : max_r])
    plt.show()


RG = RequestGenerator(10000, 1)
RP = RequestProcessor(10, RG.getRequestList())
RP.processRequests(RG.getRequestList())

times = []
for r in RG.getRequestList():
    times.append(r.getWorkTime())

CT = RP.getComputationTimes()
#SC = SortedCollection(RP.getTimeline(), TimelineEvent.getEventTime)
    
print(len(RG.getRequestList()))        

#r = Request("as", 0)
#q = Request("ds", 1)       
#print(r.getRequestType())
#print(q.getRequestType())