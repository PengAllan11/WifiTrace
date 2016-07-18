from mongoengine import *

from MongoUtil import *

MongoInst = MongoUtil(database='Wenzhou')

macResults = Wifi.objects().distinct(field='mac')


for tempMac in macResults:
    wifiRecords = Wifi.objects(mac=tempMac)
    if(len(wifiRecords)) == 1:
        continue
    else:
        sortedRecords = MongoInst.sortTrace(wifiRecords)
        currentTrace = Trace(mac=tempMac)
        currentTraceRecords = []
        currentTraceInTimes = []
        currentTraceOutTimes = []
        printString = tempMac + ' : '
        for tempRecord in sortedRecords:
            currentTraceRecords.append(tempRecord.device_id)
            currentTraceInTimes.append(tempRecord.in_time)
            currentTraceOutTimes.append(tempRecord.out_time)
            printString += tempRecord.device_id + "->"
        currentTrace.trace_record = currentTraceRecords
        currentTrace.in_times = currentTraceInTimes
        currentTrace.out_times = currentTraceOutTimes
        currentTrace.save()
        print(printString)