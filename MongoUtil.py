from mongoengine import *
import json
import datetime

# structure of collection wifi
class Wifi(Document):
    device_id = StringField(default='')
    mac = StringField(default='')
    in_time = StringField(default='')
    out_time = StringField(default='')
    pwr = StringField(default='')
    max_pwr = StringField(default='')
    min_pwr = StringField(default='')
    src_catch_times = StringField(default='')
    dst_catch_times = StringField(default='')
    channel = StringField(default='')
    direction = StringField(default='')
    gps1 = StringField(default='')
    gps2 = StringField(default='')
    gps3 = StringField(default='')
    gps4 = StringField(default='')
    gps5 = StringField(default='')
    create_date = StringField(default='')


# structure of collection device
class Device(Document):
    device_id = StringField(default='')
    name = StringField(default='')
    reg_date = DateTimeField(default='')
    receive_date = DateTimeField(default='')
    install_date = DateTimeField(default='')
    district = StringField(default='')
    remove_date = DateTimeField(default='')
    device_status = StringField(default='')
    pcs = StringField(default='')
    r_data_week = StringField(default='')
    address = StringField(default='')
    service_id = StringField(default='')
    orientation = StringField(default='')
    number_3g = StringField(default='')
    marks = StringField(default='')
    maxl_date = StringField(default='')
    message = StringField(default='')
    maxr_date = StringField(default='')
    new_name = StringField(default='')
    order_no = StringField(default='')
    attr = StringField(default='')
    ga_bak = StringField(default='')
    map_xdevice_id = StringField(default='')
    longitude = StringField(default='')
    latitude = StringField(default='')
    place_type = StringField(default='')
    contact_no = StringField(default='')
    name_2 = StringField(default='')
    forced_state = StringField(default='')

class Trace(Document):
    mac = StringField(default='')
    trace_record = ListField(default=[])
    in_times = ListField(default=[])
    out_times = ListField(default=[])

class MongoUtil(object):
    # define connection params
    def __init__(self, host='192.168.0.230', port=27017, username='', password='', database=''):
        self.configuration = dict(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database
        )
        self.__createNewConnection()
        self.__exceptions = set()

    # establish connection
    def __createNewConnection(self):
        connect(db=self.configuration.get('database'),
                host=self.configuration.get('host'),
                port=self.configuration.get('port'),
                username=self.configuration.get('username'),
                password=self.configuration.get('password'))

    # time in timezone
    def isTimeInRegion(self, start_time, in_time, end_time, out_time, delta_time):
        d_start_time = datetime.datetime.strptime(start_time, "%Y%m%d%H%M%S")
        d_end_time = datetime.datetime.strptime(end_time, "%Y%m%d%H%M%S")
        d_in_time = datetime.datetime.strptime(in_time, "%Y%m%d%H%M%S")
        d_out_time = datetime.datetime.strptime(out_time, "%Y%m%d%H%M%S")
        d_lower_time = d_start_time - datetime.timedelta(seconds=delta_time)

        if d_in_time >= d_lower_time and d_out_time <= d_end_time:
            return True
        else:
            return False

    def calculateDeltaTime(self, start_time, end_time):
        d_start = datetime.datetime.strptime(start_time, "%Y%m%d%H%M%S")
        d_end = datetime.datetime.strptime(end_time, "%Y%m%d%H%M%S")
        return (d_end - d_start).seconds

    # query wifi record by input options
    def findWifiRecords(self, mac, delta_time, start_time=-1, end_time=-1, duration=-1, intersection=-1, exact_or_not = 1, exact_date=-1):
        # get all records by mac address
        wifiRecords = Wifi.objects(mac=mac)
        resultWifiRecords = []

        if len(wifiRecords) == 0:
            return json.dumps({'error': True, 'error_code': 001, 'error_msg': 'Cannot find this mac address'}, indent=4)
        else:
            for tempRecords in wifiRecords:
                if start_time == -1 or end_time == -1:
                    bool_Region = True
                else:
                    bool_Region = self.isTimeInRegion(start_time, tempRecords.in_time,
                                                      end_time, tempRecords.out_time, delta_time)

                if exact_date == -1:
                    bool_Exact = True
                else:
                    bool_Exact = self.isTimeInRegion(str(exact_date) + '000000', tempRecords.in_time,
                                                 str(exact_date) + '235959', tempRecords.out_time, 0)

                if bool_Region or(exact_date != -1 and bool_Exact):
                    if duration == -1:
                        resultWifiRecords.append(tempRecords)
                    else:
                        current_duration = self.calculateDeltaTime(tempRecords.in_time, tempRecords.out_time)
                        if not (0 < current_duration <= duration):
                            continue
                        else:
                            resultWifiRecords.append(tempRecords)
                else:
                    continue

            return self.findSimilarTrace(resultWifiRecords, start_time, end_time, delta_time, duration, intersection, exact_or_not)

    def findSimilarTrace(self, records, start_time, end_time, delta_time, duration, intersection, exact_or_not):

        if len(records) == 1:
            return self.onceMacAdress(records, start_time, end_time, delta_time, duration)
        elif len(records) >= 1:
            return self.moreMacAddress(records, start_time, end_time, delta_time, duration, intersection, exact_or_not)
        else:
            return json.dumps({'error': True, 'error_code': 001, 'error_msg': 'Cannot find this mac address'}, indent=4)

    def moreMacAddress(self, records, start_time, end_time, delta_time, duration, intersection, exact_or_not):
        # sort trace in current occasion
        records = self.sortTrace(records)

        targetTrace = []
        for tempRecord in records:
            targetTrace.append(tempRecord.device_id)

        # case 1 : exact_or_not = 1, intersection = -1 means equal
        if intersection == -1 and exact_or_not == 1:
            print('case 1 : exact_or_not = 1, intersection = -1 means equal')
            traceRecords = Trace.objects()
            returnResult = []
            for tempTrace in traceRecords:
                if len(tempTrace.trace_record) < len(targetTrace) or tempTrace.mac == records[0].mac:
                    continue
                else:
                    indexOfTrace = self.indexOfList(targetTrace, tempTrace.trace_record)
                    if len(indexOfTrace) == 0:
                        continue
                    else:
                        for temp in indexOfTrace:

                            if (start_time == -1 or end_time == -1) or (self.isTimeInRegion(start_time, tempTrace.in_times[temp], end_time, tempTrace.out_times[temp+len(targetTrace)-1], delta_time)):
                                if duration == -1:
                                    currentTrace = {
                                        'mac': tempTrace.mac,
                                        'start_index': temp,
                                        'end_index': temp + len(targetTrace) -1
                                    }
                                    returnResult.append(currentTrace)
                                else:
                                    iFlag = 0
                                    for iIndex in range(temp, temp + len(targetTrace) -1):
                                        current_duration = self.calculateDeltaTime(tempTrace.in_times[iIndex], tempTrace.out_times[iIndex])
                                        if not (0 < current_duration <= duration):
                                            iFlag = 1
                                            break

                                    if iFlag == 0:
                                        currentTrace = {
                                            'mac': tempTrace.mac,
                                            'start_index': temp,
                                            'end_index': temp + len(targetTrace) - 1
                                         }
                                        returnResult.append(currentTrace)
                            else:
                                continue

            if len(returnResult) == 0:
                return json.dumps({'error': True, 'error_code': 201, "error_msg": "No similar trace"}, indent=4)
            else:
                return self.generateJsonDocument(returnResult, targetTrace)

        # case 2 : exact_or_not = 1, intersection != -1 means find subset greater than intersection
        if intersection != -1 and exact_or_not == 1:
            print('case 2 : exact_or_not = 1, intersection != -1 means find subset greater than intersection')

            if len(targetTrace) < intersection:
                return json.dumps({'error': True, 'error_code': 202, 'error_msg': 'intersection is greater than trace length'})
            else:
                tarceRecords = Trace.objects()
                returnResult = []
                subTraces = self.allSubsetTraces(targetTrace, intersection)
                for tempTrace in tarceRecords:
                    if len(tempTrace.trace_record) < intersection or tempTrace.mac == records[0].mac:
                        continue
                    else:
                        for tempSubTrace in subTraces:
                            indexOfTrace = self.indexOfList(tempSubTrace, tempTrace.trace_record)
                            if len(indexOfTrace) == 0:
                                continue
                            else:
                                for temp in indexOfTrace:

                                    if (start_time == -1 or end_time == -1) or (self.isTimeInRegion(start_time, tempTrace.in_times[temp], end_time, tempTrace.out_times[temp+len(targetTrace)-1], delta_time)):
                                        if duration == -1:
                                            currentTrace = {
                                                'mac': tempTrace.mac,
                                                'start_index': temp,
                                                'end_index': temp + len(tempSubTrace) -1
                                            }
                                            returnResult.append(currentTrace)
                                        else:
                                            iFlag = 0
                                            for iIndex in range(temp, temp + len(tempSubTrace) -1):
                                                current_duration = self.calculateDeltaTime(tempTrace.in_times[iIndex], tempTrace.out_times[iIndex])
                                                if not (0 < current_duration <= duration):
                                                    iFlag = 1
                                                    break

                                            if iFlag == 0:
                                                currentTrace = {
                                                    'mac': tempTrace.mac,
                                                    'start_index': temp,
                                                    'end_index': temp + len(tempSubTrace) - 1
                                                 }
                                                returnResult.append(currentTrace)
                                    else:
                                        continue
                if len(returnResult) == 0:
                    return json.dumps({'error': True, 'error_code': 201, "error_msg": "No similar trace"}, indent=4)
                else:
                    return self.generateJsonDocument(returnResult, targetTrace)

        # case 3 : exact_or_not = 0, intersection == -1 means ignore trace order
        if  exact_or_not == 0:
            print('case 3 : exact_or_not = 1, intersection = -1 means ignore trace order')
            traceRecords = Trace.objects()
            returnResult = []
            for tempTrace in traceRecords:
                if len(tempTrace.trace_record) < len(targetTrace) or tempTrace.mac == records[0].mac:
                    continue
                else:
                    if set(tempTrace.trace_record) >= set(targetTrace):

                        if start_time == -1 or end_time == -1:
                            result = self.divideSubTrace(tempTrace.trace_record, targetTrace, 0, len(tempTrace.trace_record))

                            currentResult = {
                                'mac': tempTrace.mac,
                                'start_index': result['start'],
                                'end_index': result['end']
                            }

                            returnResult.append(currentResult)

                        else:
                            indexOfTimeRegion = self.findTraceInTimeRegion(tempTrace, start_time, end_time, delta_time)
                            iStart = indexOfTimeRegion['index_start']
                            iEnd = indexOfTimeRegion['index_end']
                            subTraceList = tempTrace.trace_record[iStart, iEnd]
                            if set(subTraceList) >= set(targetTrace):
                                result = self.divideSubTrace(tempTrace.trace_record, targetTrace, iStart, iEnd)
                                currentResult = {
                                    'mac': tempTrace.mac,
                                    'start_index': result['start'],
                                    'end_index': result['end']
                                }

                                returnResult.append(currentResult)
                            else:
                                continue
                    else:
                        continue

            if len(returnResult) == 0:
                return json.dumps({'error': True, 'error_code': 201, "error_msg": "No similar trace"}, indent=4)
            else:
                return self.generateJsonDocument(returnResult, targetTrace)

    def generateJsonDocument(self, returnResult, targetTrace):
        listTrace = []
        for temp in returnResult:
            currTrace = Trace.objects(mac=temp['mac']).first()
            dicTrace = {
                'mac': temp['mac'],
                'trace': currTrace.trace_record[temp['start_index']: temp['end_index']+1],
                'in_times': currTrace.in_times[temp['start_index']: temp['end_index']+1],
                'out_times': currTrace.out_times[temp['start_index']: temp['end_index']+1]
            }
            listTrace.append(dicTrace)
        jsonResult = {
            'error': False,
            'target': targetTrace,
            'result': listTrace
        }
        return json.dumps(jsonResult, indent=4)

    def divideSubTrace(self, source, target, index_start, index_end):

        startList = []
        endList = []
        for tempTarget in target:
            startList.append(source.index(tempTarget, index_start, index_end))

        reverse_source = source[::-1]
        for tempTarget in target:
            endList.append(len(reverse_source) - 1 - reverse_source.index(tempTarget, len(source)-index_end, len(source) - index_start))

        return {'start': min(startList), 'end': max(endList)}



    def findTraceInTimeRegion(self, trace, start_time, end_time, delta_time):
        d_start_time = datetime.datetime.strptime(start_time, "%Y%m%d%H%M%S")
        index_start = 0
        index_end = 0
        for tempIn in trace.in_times:
            d_temp_in = datetime.datetime.strptime(tempIn, "%Y%m%d%H%M%S")
            if d_temp_in >= d_start_time -  datetime.timedelta(seconds=delta_time):
                break
            index_start += 1
        for tempOut in trace.out_times:
            if tempOut >= end_time:
                break
            index_end += 1

        return {'index_start': index_start, 'index_end': index_end}

    def allSubsetTraces(self, trace, intersection):
        result = []
        for iTemp in range(intersection, len(trace)+1):
            subeset = self.subsetOfTrace(trace, iTemp)
            for temp in subeset:
                result.append(temp)
        return result

    def subsetOfTrace(self, trace, intersection):
        result = []
        for temp in range(0, len(trace)):
            if temp + intersection <= len(trace):
                tempList = []
                for i in range(0, intersection):
                    tempList.append(trace[temp + i])

                result.append(tempList)
            else:
                break

        return result

    def indexOfList(self, target, source):
        result = []
        for i in range(0, len(source)):
            if source[i] == target[0]:
                iFlag = 1
                for j in range(0, len(target)):
                    if (i + j < len(source) and source[i + j] != target[j]) or i + len(target) > len(source):
                        iFlag = 0
                        break
                if iFlag == 1:
                    result.append(i)
        return result

    def sortSortSort(self, record):
        for i in range(len(record)-1):
            for j in range(i + 1, len(record)):
                if record[i] > record[j]:
                    tempI = record[i]
                    tempJ = record[j]
                    record[i] = tempJ
                    record[j] = tempI
        return record


    def sortTrace(self, inRecords):
        records = []
        for tempRecords in inRecords:
            records.append(tempRecords)
        for i in range(len(records) - 1):
            for j in range(i + 1, len(records)):
                if records[i].in_time > records[j].in_time:
                    temp = records[i]
                    records[i] = records[j]
                    records[j] = temp

        return records

    def getDevices(self,devicesList):

        resultDic = {}
        for tempDevice in devicesList:
            deviceInfo = Device.objects(device_id=tempDevice)
            if len(deviceInfo) != 1:
                resultDic[tempDevice] = None
            else:
                x = deviceInfo[0].longitude
                y = deviceInfo[0].latitude
                resultDic[tempDevice] = {'longitude': x, 'latitude': y}

        resultDic['error'] = False

        return json.dumps(resultDic, indent=4)


    def getDeviceInfoById(self, deviceId):
        deviceInfo = Device.objects(device_id=deviceId)

        if len(deviceInfo) != 1:
            return json.dumps({'error': True, 'error_code': 309, 'error_msg': 'can not find device info'}, indent=4)
        else:
            device = deviceInfo[0]
            jsonDeviceInfoString = {
                "device_id": device.device_id,
                "name": device.name,
                "reg_date": device.reg_date,
                "receive_date": device.receive_date,
                "install_date": device.install_date,
                "district": device.district,
                "remove_date": device.remove_date,
                "device_status": device.device_status,
                "pcs": device.pcs,
                "r_data_week": device.r_data_week,
                "address": device.address,
                "service_id": device.service_id,
                "orientation": device.orientation,
                "number_3g": device.number_3g,
                "marks": device.marks,
                "maxl_date": device.maxl_date,
                "new_name": device.new_name,
                "order_no": device.order_no,
                "attr": device.attr,
                "ga_bak": device.ga_bak,
                "map_xdevice_id": device.map_xdevice_id,
                "longitude": device.longitude,
                "latitude": device.latitude,
                "place_type": device.place_type,
                "contact_no": device.contact_no,
                "forced_state": device.forced_state
            }
            return json.dumps({'error': False, 'deviceInfo': jsonDeviceInfoString},indent=4)


    def onceMacAdress(self, records, start_time, end_time, delta_time, duration):
        wifiRecord = records[0]
        deviceInfo = Device.objects(device_id=wifiRecord.device_id)
        jsonDeviceCode = 0
        jsonOtherWifiCode = 0
        jsonResult = {}
        # get device info
        if len(deviceInfo) != 1:
            jsonDeviceCode = 101
        else:
            device = deviceInfo[0]
            jsonDeviceInfoString = {
                "device_id": device.device_id,
                "name": device.name,
                "reg_date": device.reg_date,
                "receive_date": device.receive_date,
                "install_date": device.install_date,
                "district": device.district,
                "remove_date": device.remove_date,
                "device_status": device.device_status,
                "pcs": device.pcs,
                "r_data_week": device.r_data_week,
                "address": device.address,
                "service_id": device.service_id,
                "orientation": device.orientation,
                "number_3g": device.number_3g,
                "marks": device.marks,
                "maxl_date": device.maxl_date,
                "new_name": device.new_name,
                "order_no": device.order_no,
                "attr": device.attr,
                "ga_bak": device.ga_bak,
                "map_xdevice_id": device.map_xdevice_id,
                "longitude": device.longitude,
                "latitude": device.latitude,
                "place_type": device.place_type,
                "contact_no": device.contact_no,
                "forced_state": device.forced_state
            }

        # get other persons wifi records
        otherWifiRecords = Wifi.objects(device_id=wifiRecord.device_id)

        if len(otherWifiRecords) >= 1:
            jsonOtherWifiRecord = []
            for tempRecord in otherWifiRecords:
                if tempRecord.mac == records[0].mac:
                    continue
                currRecord = {
                    "mac" : tempRecord.mac,
                    "in_time" : tempRecord.in_time,
                    "out_time" : tempRecord.out_time,
                    "pwr" : tempRecord.pwr,
                    "max_pwr" : tempRecord.max_pwr,
                    "min_pwr" : tempRecord.min_pwr,
                    "src_catch_times" : tempRecord.src_catch_times,
                    "dst_catch_times" : tempRecord.dst_catch_times,
                    "channel" : tempRecord.channel,
                    "direction" : tempRecord.direction,
                    "gps1" : tempRecord.gps1,
                    "gps2" : tempRecord.gps2,
                    "gps3" : tempRecord.gps3,
                    "gps4" : tempRecord.gps4,
                    "gps5" : tempRecord.gps5,
                    "create_date" : tempRecord.create_date
                }

                if start_time == -1 or end_time == -1:
                    if duration == -1:
                        jsonOtherWifiRecord.append(currRecord)
                    else:
                        if 0 < self.calculateDeltaTime(tempRecord.in_time, tempRecord.out_time) <= duration:
                            jsonOtherWifiRecord.append(currRecord)
                        else:
                            continue
                else:
                    if self.isTimeInRegion(start_time, tempRecord.in_time, end_time, tempRecord.out_time, delta_time):
                        if duration == -1:
                            jsonOtherWifiRecord.append(currRecord)
                        else:
                            if 0 < self.calculateDeltaTime(tempRecord.in_time, tempRecord.out_time) <= duration:
                                jsonOtherWifiRecord.append(currRecord)
                            else:
                                continue
                    else:
                        continue
        else:
            jsonOtherWifiCode = 102

        if jsonDeviceCode == 0 and jsonOtherWifiCode == 0:
            jsonResult['error'] = False
            jsonResult['target'] = [records[0].device_id]
            jsonResult['device'] = jsonDeviceInfoString
            jsonResult['others'] = jsonOtherWifiRecord
        else:
            jsonResult['error'] = True
            if jsonDeviceCode != 0 and jsonOtherWifiCode == 0:
                jsonResult['error_code'] = 101
                jsonResult['error_msg'] = 'cannot find this device'
                jsonResult['others'] = jsonOtherWifiRecord
            elif jsonDeviceCode == 0 and jsonOtherWifiCode != 0:
                jsonResult['error_code'] = 102
                jsonResult['error_msg'] = 'cannot find other records'
                jsonResult['device'] = jsonDeviceInfoString
            else:
                jsonResult['error_code'] = 102
                jsonResult['error_msg'] = 'no device and no records'

        return json.dumps(jsonResult, indent=4)
