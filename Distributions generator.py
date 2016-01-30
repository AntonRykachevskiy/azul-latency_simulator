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
    core_release = 3
    core_occupy = 4
    start_garbage_collector = 5
    stop_garbage_collector = 6


class RequestType(Enum):
    simple_request = 1


class CoreStatus(Enum):
    free = 1
    busy = 2


class GarbageCollector:
    def __init__ (self, start_percentage = 0.9, stop_percentage = 0.0):
        self.__start_percentage = start_percentage
        self.__stop_percentage = stop_percentage
    def getStartPercentage(self):
        return self.__start_percentage
    def getStopPercentage(self):
        return self.__stop_percentage 
        
class Request:
    def __init__ (self, request_type = None, start_time = None, request_num = None,
                  computation_difficulty = 0, RAM_usage = 0):
        self.__request_type = request_type
        self.__start_time = start_time
        self.__request_num = request_num
        self.__end_time = None
        self.__computation_difficulty = computation_difficulty
        self.__RAM_usage = RAM_usage
    def setRequestEndTime(self, end_time):
        self.__end_time = end_time
    def getRequestEndTime(self):
        return self.__end_time
    def getRequestType(self) :
        return self.__request_type
    def getRequestStartTime(self):
        return self.__start_time
    def getRequestNumber(self):
        return self.__request_num
    def getComputationDifficulty(self):
        return self.__computation_difficulty
    def getRAMUsage(self):
        return self.__RAM_usage
    def printRequest(self):
        print(self.__request_type, self.__start_time, self.__end_time, self.__request_num)    
    def getWorkTime(self):
        return self.__end_time - self.__start_time
            
class RequestGenerator:
    """- throughput in req per sec - end time in sec - request_list in microseconds """
    def __init__ (self, throughput = None, end_time = None, computation_difficulty = 0, RAM_usage = 0):
        microsecond = 1000000         
        self.__request_list = []
        if not (throughput is None) and not (end_time is None):
            for i in range(int(end_time * throughput)):

                request = Request("sr", float(microsecond * i / throughput), i,
                                  computation_difficulty = computation_difficulty,
                                  RAM_usage = RAM_usage)
                self.__request_list.append(request)        
    def getRequestList(self):
        return self.__request_list


class TimelineEvent:
    def __init__(self, event_time = None, event_type = None):
        self.__event_time = event_time
        self.__event_type = event_type
    def getEventTime(self):
        return self.__event_time
    def getEventType(self):
        return self.__event_type
    def resetEventTime(self, new_event_time):
        self.__event_time = new_event_time
    def printEvent(self):
        print(self.__event_type, self.__event_time)

class RequestEvent(TimelineEvent):
    def __init__(self,  event_time = None, event_type = None, request_number = None):
        TimelineEvent.__init__(self, event_time, event_type)
        self.__request_number = request_number
    def getRequestNumber(self):
        return self.__request_number
    def printEvent(self):
        TimelineEvent.printEvent(self)
        print(self.__request_number)

class CoreEvent(TimelineEvent):
    def __init__(self,  event_time = None, event_type = None, core_id = None):
        TimelineEvent.__init__(self, event_time, event_type)
        self.__core_id = core_id
    def getCoreId(self):
        return self.__core_id
    def printEvent(self):
        TimelineEvent.printEvent(self)
        print(self.__core_id)

class GCEvent(TimelineEvent):
    pass

class Core:
    def __init__ (self, core_id = 0):
        self.__status = CoreStatus.free
        self.__core_id = core_id
        self.__current_request = None

    def processRequest(self, request, start_time):
        self.__status = CoreStatus.busy
        self.__current_request = request
        computation_time_error_mean = 0
        computation_time_error_sigma = 20
        computation_time_error = normalvariate(computation_time_error_mean, computation_time_error_sigma)
        computation_time = request.getComputationDifficulty() + computation_time_error
        
        self.__core_occupation_event = CoreEvent(start_time, EventType.core_occupy, self.__core_id)
        end_time = start_time + computation_time
        self.__current_request.setRequestEndTime(end_time)
        self.__request_finish_event = RequestEvent(end_time, EventType.end_request, request.getRequestNumber())
        self.__core_release_event = CoreEvent(end_time, EventType.core_release, self.__core_id)
        
        return self.__core_occupation_event, self.__request_finish_event, self.__core_release_event
    
    def pauseProcessing(self, pause_time):
        current_request_finish_event = self.__request_finish_event
        current_core_release_event = self.__core_release_event

        self.__current_request.setRequestEndTime(self.__current_request.getRequestEndTime() + pause_time)
        self.__request_finish_event.resetEventTime(self.__request_finish_event.getEventTime() + pause_time)
        self.__core_release_event.resetEventTime(self.__core_release_event.getEventTime() + pause_time)
        
        return (current_request_finish_event, current_core_release_event), (self.__request_finish_event, self.__core_release_event)
        
    def releaseCore(self):
        processed_request = self.__current_request
        self.__status = CoreStatus.free
        self.__request_finish_event = None
        self.__current_request = None
        self.__core_release_event = None
        self.__core_occupation_event = None
        return processed_request

    def getCoreStatus(self):
        return self.__status

    def getCurrentRequest(self):
        return self.__current_request
        
        
class RequestProcessor:

    def __init__ (self, cores_number = 1, request_list = None, RAM = 4000):
        self.__timeline = SortedCollection([], TimelineEvent.getEventTime)
        self.__cores_number = cores_number
        self.__free_cores = cores_number        
        self.__cores = [Core(i) for i in range(cores_number)]

        self.__queue = []

        self.__RAM = RAM
        self.__free_RAM = RAM
        self.__garbage_collector = GarbageCollector()
        self.__garbage_collector_status = False
        self.__garbage_collector_session = (None, None)

        self.__processed_request_list = []

        if request_list != None:
            for request in request_list:
                event = RequestEvent(request.getRequestStartTime(), EventType.new_request, request.getRequestNumber())
                self.__timeline.insert(event)
        self.__computation_times = []
        
    def __getRAMLoad__(self):
        return (self.__RAM - self.__free_RAM) / self.__RAM
    
    def __getFirstFreeCore__(self):
        'returns first free core, or -1 if all cores are busy'
        core_number = 0
        for core in self.__cores:
            if core.getCoreStatus() == CoreStatus.free:
                return core_number
            else:
                core_number += 1
        return -1

    def startGC(self, current_time):
        self.__garbage_collector_status = True

        start_garbage_collector = GCEvent(current_time, EventType.start_garbage_collector)
        time_coefficient = 2
        collecting_time = (self.__getRAMLoad__() - self.__garbage_collector.getStopPercentage()) * self.__RAM * time_coefficient
        stop_garbage_collector = GCEvent(current_time + collecting_time, EventType.stop_garbage_collector)

        self.__garbage_collector_session = (start_garbage_collector, stop_garbage_collector)
        self.__timeline.insert_right(start_garbage_collector)
        self.__timeline.insert_right(stop_garbage_collector)
            
    def processNewRequest(self, request, new_request_event):
        free_core = self.__getFirstFreeCore__()
        #print self.__queue
        #print self.__garbage_collector_status

        #print(self.__free_cores)
        if self.__garbage_collector_status:
            self.__queue.append(request)
        elif self.__getRAMLoad__() > self.__garbage_collector.getStartPercentage():
            self.startGC(new_request_event.getEventTime())
            self.__queue.append(request)
        elif self.__free_cores >= 1:
            self.__free_cores -= 1
            self.__free_RAM -= request.getRAMUsage()
            core_occupation_event, request_finish_event, core_release_event = \
                self.__cores[free_core].processRequest(request, new_request_event.getEventTime())
            self.__timeline.insert_right(core_occupation_event)
            self.__timeline.insert(request_finish_event)
            self.__timeline.insert_right(core_release_event)
        else:
            self.__queue.append(request)
    
    def processCoreRelease(self, core_release_event):
        if not self.__garbage_collector_status:
            self.__free_cores += 1
            processed_request =self.__cores[core_release_event.getCoreId()].releaseCore()

            self.__processed_request_list.append(processed_request)

            if self.__queue:
                request = self.__queue.pop(0)
                self.processNewRequest(request, core_release_event)
        else:
            self.__timeline.remove(core_release_event)
            delay = core_release_event.getEventTime() - self.__garbage_collector_session[0].getEventTime()
            core_release_event.resetEventTime(self.__garbage_collector_session[1].getEventTime() + delay)
            self.__timeline.insert_right(core_release_event)

    def processGSStop(self, gc_stop_event):
        #print ("STOPPED")
        self.__garbage_collector_status = False
        self.__garbage_collector_session = (None, None)
        self.__free_RAM = self.__RAM * (1 - self.__garbage_collector.getStopPercentage())
        if self.__queue and self.__getFirstFreeCore__ != -1:
            request = self.__queue.pop(0)
            self.processNewRequest(request, gc_stop_event)
            
        
    def processRequests(self, request_list = None):
        if request_list: 
            for event in self.__timeline:
                if event.getEventType() == EventType.new_request:
                    self.processNewRequest(request_list[event.getRequestNumber()], event)
                if event.getEventType() == EventType.core_release:
                    self.processCoreRelease(event)
                if event.getEventType() == EventType.stop_garbage_collector:
                    self.processGSStop(event)
        
    def getTimeline(self):
        return self.__timeline
        
    def printTimeline(self):
        for event in self.__timeline:
            event.printEvent()
    
    def getComputationTimes(self):
        return self.__computation_times

    def getProcessedRequestList(self):
        return self.__processed_request_list
              
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


RG = RequestGenerator(1000, 10 , 1000, 100)
RP = RequestProcessor(10, RG.getRequestList())
RP.processRequests(RG.getRequestList())

times = []
for r in RP.getProcessedRequestList():
    times.append(r.getWorkTime())

CT = RP.getComputationTimes()
#SC = SortedCollection(RP.getTimeline(), TimelineEvent.getEventTime)
    
print(len(RG.getRequestList())) 
print(len(times))       
plotDistribution(times)

#print(times)
#r = Request("as", 0)
#q = Request("ds", 1)       
#print(r.getRequestType())
#print(q.getRequestType())
