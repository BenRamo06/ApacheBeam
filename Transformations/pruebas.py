from datetime import datetime
import time


# dates = datetime.utcnow().strftime
dates = datetime.strptime('2021-09-22 02:52:39', '%Y-%m-%d %H:%M:%S')

print(dates)
print(int(time.mktime(dates.timetuple())))
print(int(time.mktime(dates.timetuple()))*1000)
print(int(time.mktime(dates.timetuple()))*1000000)


