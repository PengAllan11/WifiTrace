# from mongoengine import *
# from MongoUtil import Wifi, MongoUtil
# import datetime
#
# connect('Wenzhou')
#
# records = Wifi.objects(mac='E001374')
#
# d1 = records[0].in_time
# d2 = datetime.datetime.strptime(d1, "%Y%m%d%H%M%S")
# dInputTime = datetime.datetime.strptime('20150101010101',"%Y%m%d%H%M%S")
# d3 = d2 + datetime.timedelta(hours=3)
# d4 = d2 - datetime.timedelta(hours=3)
# print((d3-d4).seconds)
#
#
# print(d3)
# print(datetime.datetime.strptime(d1, "%Y%m%d%H%M%S"))

#
#
# class Wifi(DynamicDocument):
#     device_id = StringField(default= '')

# new = Wifi(device_id = 'HELLO')
# new.mark = 1
#
# new.save()

#
# for wifiRecord in Wifi.objects:
#     print(wifiRecord.device_id)

from MongoUtil import *

MongoInst = MongoUtil(database='Wenzhou')

# str1 = "20150103000000"
# str2 = "20141231000000"
#
# d_time_i = datetime.datetime.strptime(str1, "%Y%m%d%H%M%S")
# d_time_j = datetime.datetime.strptime(str2, "%Y%m%d%H%M%S")


# print(MongoInst.calculateDeltaTime(''))
# result = MongoInst.findWifiRecords('E001374', 120, -1, '20150201000000', intersection=2, exact_or_not=0)
# result = MongoInst.findWifiRecords('E001374', 120, '20150101000000', -1, intersection=-1, exact_or_not= 1)
# result = MongoInst.findWifiRecords('E001372', 120, -1, '20150201000000', intersection=2, exact_or_not=1)
result = MongoInst.findWifiRecords('E0013757', 120, '20150101000000', -1, intersection=-1, exact_or_not= 1)
print(result)

print(int('-1'))

def md5(str):
    import hashlib
    m = hashlib.md5()
    m.update(str)
    return m.hexdigest()
a = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
print(a[0:11])
print(md5(a[0:11]))

string = {'mark': 1, 'yang': 2}

strJson = json.dumps(string)

deJson = json.loads(strJson)

list1 = [1, 2, 3]
print(MongoInst.getDevices(['330L038']))

