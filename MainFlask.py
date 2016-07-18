from pprint import pprint

from flask import Flask, request
from MongoUtil import MongoUtil
import datetime
import json
app = Flask(__name__)

@app.route('/')
def index():
    print('Hello')
    return 'Hello World'

@app.route('/IAGraph/Wenzhou/WifiRoute/<mac>/<delta>/<start_time>/<end_time>/<duration>/<intersection>/<exact>/<token>')
def search(mac, delta, start_time, end_time, duration, intersection, exact, token):
    currentTime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    password = currentTime[0:11]
    if token == md5(password):
        return json.dumps({'error': True, 'errorMsg': 'Incorrect Token'})

    MongoInst = MongoUtil(database='Wenzhou')
    if start_time == '-1':
        start_time = -1
    if end_time == '-1':
        end_time == -1
    result = MongoInst.findWifiRecords(mac=mac, delta_time=int(delta), start_time=start_time, end_time=end_time, duration=int(duration), intersection=int(intersection), exact_or_not=int(exact))
    return result

@app.route('/IAGraph/Wenzhou/Device/<deviceid>/<token>')
def getDeviceId(deviceid, token):

    currentTime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    password = currentTime[0:11]
    if token == md5(password):
        return json.dumps({'error': True, 'error_code': 305, 'error_msg': 'invalid token'})
    MongoInst = MongoUtil(database='Wenzhou')
    return MongoInst.getDeviceInfoById(deviceid)

@app.route('/IAGraph/Wenzhou/Devices', methods=['post'])
def getDevices():
    if request.get_json():
        inputJson = request.get_json()
        MongoInst = MongoUtil(database='Wenzhou')
        if not inputJson.has_key('token'):
            return json.dumps({'error': True, 'error_code': 305, 'error_msg': 'input token can not be null'}, indent=4)
        else:
            token = inputJson['token']
            currentTime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            password = currentTime[0:11]
            if token == md5(password):
                return json.dumps({'error': True, 'error_code': 305, 'error_msg': 'invalid token'}, indent=4)

        if not inputJson.has_key('devices'):
            return json.dumps({'error': True, 'error_code': 307, 'error_msg': 'devices list is null'}, indent=4)
        else:
            devices = inputJson['devices']
            if not type(devices) == list:
                return json.dumps({'error': True, 'error_code': 308, 'error_msg': 'devices is not a list'}, indent=4)
            else:
                result = MongoInst.getDevices(devices)
                return result
    else:
        return json.dumps({'error': True, 'error_code': 306, 'error_msg': 'input json is null or content-type is wrong'})

@app.route('/mark/<test>')
def Mark(test):
    currentTime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    password = currentTime[0:11]
    if test == 'mark':
        return md5(password)
    else:
        return 'NO'

@app.route('/IAGraph/Wenzhou/Wifi/Trace', methods=['get'])
def findTrace():

    # token
    if not request.args.get('token'):
        return json.dumps({'error': True, 'error_code': 304, 'error_msg': 'input token can not be null'}, indent=4)
    else:
        currentTime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        password = currentTime[0:11]
        if request.args.get('token') == md5(password):
            return json.dumps({'error': True, 'error_code': 305, 'error_msg': 'invalid token'}, indent=4)


    # mac address
    if not request.args.get('mac'):
        return json.dumps({'error': True, 'error_code': 301, 'error_msg': 'input mac address can not be null'}, indent=4)
    else:
        mac = request.args.get('mac')

    # start_time
    if not request.args.get('start_time'):
        start_time = -1
    else:
        if len(request.args.get('start_time')) != 14:
            return json.dumps({'error': True, 'error_code': 302, 'error_msg': 'format of input start_time is wrong '}, indent=4)
        else:
            start_time = request.args.get('start_time')

    # end_time
    if not request.args.get('end_time'):
        end_time = -1
    else:
        if len(request.args.get('end_time')) != 14:
            return json.dumps({'error': True, 'error_code': 303, 'error_msg': 'format of input end_time is wrong '}, indent=4)
        else:
            end_time = request.args.get('end_time')

    # delta_time
    if not request.args.get('delta_time'):
        delta_time = 1*60
    else:
        delta_time = int(request.args.get('delta_time'))*60

    # duration
    if not request.args.get('duration'):
        duration = -1
    else:
        duration = int(request.args.get('duration')) * 60

    # intersection
    if not request.args.get('intersection'):
        intersection = -1
    else:
        intersection = int(request.args.get('intersection'))

    # exact_trace
    if not request.args.get('exact_trace'):
        exact_trace = 1
    else:
        if request.args.get('exact_trace') != '1' or request.args.get('exact_trace') != '0':
            exact_trace = 1
        else:
            exact_trace = int(request.args.get('exact_trace'))

    MongoInst = MongoUtil(database='Wenzhou')
    result = MongoInst.findWifiRecords(mac=mac, delta_time=delta_time, start_time=start_time, end_time=end_time, duration=duration, intersection=intersection, exact_or_not=exact_trace)
    return result


def md5(str):
    import hashlib
    m = hashlib.md5()
    m.update(str)
    return m.hexdigest()


if __name__ == '__main__':
    app.run()
