# -*- encoding=utf-8 -*-
import datetime
import time
import calendar


class DateUtil:

    def __init__(self):
        pass

    @staticmethod
    def add_months(dt, months):
        month = dt.month - 1 + months
        year = dt.year + month / 12
        month = month % 12 + 1
        day = min(dt.day, calendar.monthrange(year, month)[1])
        return dt.replace(year=year, month=month, day=day)

    @staticmethod
    def get_today(fmt_str='%Y%m%d'):
        now_time = datetime.datetime.now()
        return now_time.strftime(fmt_str)

    @staticmethod
    def get_date_after_days(days):
        after_date = datetime.date.today() + datetime.timedelta(days)
        return after_date

    @staticmethod
    def get_date_before_days(days):
        before_date = datetime.date.today() - datetime.timedelta(days)
        return before_date

    @staticmethod
    def add_days(date, num):
        return date + datetime.timedelta(num)

    @staticmethod
    def date_to_milli_seconds(d_date):
        str_date = d_date.strftime('%Y-%m-%d %H:%M:%S')
        fmt_date = time.strptime(str_date, '%Y-%m-%d %H:%M:%S')
        return str(int(time.mktime(fmt_date))*1000)

    @staticmethod
    def get_before_days(days):
        now_time = datetime.datetime.now()
        yes_time = now_time - datetime.timedelta(days=days)
        return yes_time.strftime('%Y%m%d')

    @staticmethod
    def get_after_days(days):
        now_time = datetime.datetime.now()
        yes_time = now_time + datetime.timedelta(days=days)
        return yes_time.strftime('%Y%m%d')

    @staticmethod
    def get_table(date_time=datetime.datetime.now()):
        today = date_time
        month = today.month
        day = today.day
        if today.day >= 30:
            day = 28
        year = today.year
        if month < 10:
            return 'flightApiLog_' + str(year) + '0' + str(month) + str(day/10)
        else:
            return 'flightApiLog_' + str(year)  + str(month) + str(day / 10)

    @staticmethod
    def get_all_table(year, month):
        table_list = []
        for i in xrange(3):
            if month < 10:
                table_list.append('flightApiLog_' + str(year) + '0' + str(month) + str(i))
            else:
                table_list.append('flightApiLog_' + str(year) + str(month) + str(i))
        return table_list

    @staticmethod
    def get_last_week_date(current_time=datetime.date.today()):
        end_weekdate = current_time - datetime.timedelta(days=current_time.weekday())
        start_weekdate = end_weekdate - datetime.timedelta(days=7)
        return start_weekdate, end_weekdate

    @staticmethod
    def get_this_week_date(current_time=datetime.date.today()):
        if not isinstance(current_time, datetime.date):
            current_time = DateUtil.str2date(current_time, '%Y-%m-%d')
        print current_time
        start_weekdate = current_time - datetime.timedelta(days=current_time.weekday())
        end_weekdate = start_weekdate + datetime.timedelta(days=7)
        return start_weekdate, end_weekdate

    @staticmethod
    def get_last_month_date(current_time=datetime.date.today()):
        end_monthdate = datetime.date(current_time.year, current_time.month, 1)
        # start_monthdate = datetime.date(currentTime.year, currentTime.month - 1, 1)
        start_monthdate = end_monthdate - datetime.timedelta(days=end_monthdate.day)
        start_monthdate = datetime.date(start_monthdate.year, start_monthdate.month, 1)
        return start_monthdate, end_monthdate

    @staticmethod
    def get_this_month_date():
        currentTime = datetime.date.today()
        start_monthdate = datetime.date(currentTime.year, currentTime.month, 1)
        # end_monthdate = datetime.date(currentTime.year, currentTime.month + 1, 1)
        end_monthdate = DateUtil.add_months(start_monthdate, 1)
        return start_monthdate, end_monthdate

    @staticmethod
    def get_last_quarter_date(current_time=datetime.date.today()):
        quarter_startmonth = [1, 4, 7, 10]
        if current_time.month < 3:
            start_quartermonth = quarter_startmonth[3]
            start_quarterdate = datetime.date(current_time.year - 1, start_quartermonth, 1)
            end_quarterdate = datetime.date(current_time.year, 1, 1)
        else:
            start_quartermonth = quarter_startmonth[current_time.month/3 - 1]
            start_quarterdate = datetime.date(current_time.year, start_quartermonth, 1)
            end_quarterdate = datetime.date(current_time.year, quarter_startmonth[current_time.month/3], 1)
        return start_quarterdate, end_quarterdate

    @staticmethod
    def get_tomorrow_day():
        now_time = datetime.datetime.now()
        yes_time = now_time + datetime.timedelta(days=1)
        return yes_time.strftime('%Y%m%d')

    @staticmethod
    def get_quarter_by_month(month):
        if (month >= 1) and (month <= 3):
            return 1
        elif (month >= 4) and (month <= 6):
            return 2
        elif (month >= 7) and (month <= 9):
            return 3
        elif (month >= 10) and (month <= 12):
            return 4

    @staticmethod
    def str2str(date_str, fmt_src, fmt_dst):
        date = datetime.datetime.strptime(date_str, fmt_src)
        return date.strftime(fmt_dst)

    @staticmethod
    def getlast_insecond(date_str_src, fmt_src):
        date_str_dest = DateUtil.str2str(date_str_src, fmt_src, '%Y-%m-%d')
        return date_str_dest + " 23:59:59"

    @staticmethod
    def getfirst_insecond(date_str_src, fmt_src):
        date_str_dest = DateUtil.str2str(date_str_src, fmt_src, '%Y-%m-%d')
        return date_str_dest + " 00:00:00"

    @staticmethod
    def get_date(year, month, day):
        date = datetime.datetime(year, month, day)

    @staticmethod
    def get_date_str(year, month, day):
        date = datetime.datetime(year, month, day)
        return DateUtil.date2str(date, '%Y-%m-%d')

    @staticmethod
    def is_leap(year):
        if year%4 != 0:
            return False
        else:
            if year%100 == 0 and year%400 != 0:
                return False
            else:
                return True

    @staticmethod
    def get_daysofmonth(year, month):
        if month in [1, 3, 5, 7, 8, 10, 12]:
            return 31
        elif month in [4, 6, 9, 11]:
            return 30
        else:
            if DateUtil.is_leap(year):
                return 29
            else:
                return 28

    @staticmethod
    def is_weekend(date_str, fmt='%Y-%m-%d'):
        date = DateUtil.str2date(date_str, fmt)
        return DateUtil.is_weekend2(date)

    @staticmethod
    def is_weekend2(date):
        dayofweek = date.isoweekday()
        if dayofweek in [6, 7]:
            return True
        else:
            return False

    @staticmethod
    def get_weekendsofmonth(year, month):
        days = DateUtil.get_daysofmonth(year, month)
        weekends = []
        for i in range(days):
            tmp_date = datetime.datetime(year, month, i+1)
            if DateUtil.is_weekend2(tmp_date):
                weekends.append(DateUtil.date2str(tmp_date, '%Y-%m-%d'))
        return weekends

    @staticmethod
    def get_worktimeofday(date):
        plus1 = datetime.timedelta(hours=8)
        plus2 = datetime.timedelta(hours=18)
        date_start = date+plus1
        date_end = date+plus2
        return DateUtil.date2str(date_start), DateUtil.date2str(date_end)

    @staticmethod
    def get_weektimeofmonth(year, month):
        days = DateUtil.get_daysofmonth(year, month)
        weekdays_start = []
        weekdays_end = []
        for i in range(days):
            tmp_date = datetime.datetime(year, month, i+1)
            if not DateUtil.is_weekend2(tmp_date):
                tmp_start, tmp_end = DateUtil.get_worktimeofday(tmp_date)
                weekdays_start.append(tmp_start)
                weekdays_end.append(tmp_end)
        return weekdays_start, weekdays_end

    @staticmethod
    def date2str(date, fmt_dst='%Y-%m-%d %H:%M:%S'):
        if not date:
            return None
        return date.strftime(fmt_dst)

    @staticmethod
    def str2date(date_str, fmt_dst='%Y-%m-%d %H:%M:%S'):
        return datetime.datetime.strptime(date_str, fmt_dst)

    @staticmethod
    def minus_days(date_str1, date_str2, fmt='%Y-%m-%d'):
        minus = DateUtil.str2date(date_str2, fmt)-DateUtil.str2date(date_str1, fmt)
        return minus.days

    @staticmethod
    def minus_insecond(date_str1, date_str2, fmt='%Y-%m-%d %H:%M:%S'):
        minus = DateUtil.str2date(date_str2, fmt) - DateUtil.str2date(date_str1, fmt)
        return minus.seconds

    @staticmethod
    def plus_hours(date_str, num, fmt='%Y-%m-%d %H:%M:%S'):
        date_dst = DateUtil.str2date(date_str, '%Y-%m-%d')+datetime.timedelta(hours=num)
        return DateUtil.date2str(date_dst, fmt)

    @staticmethod
    def minus_seconds(date, num, fmt='%Y-%m-%d %H:%M:%S'):
        date_dst = date-datetime.timedelta(seconds=num)
        return DateUtil.date2str(date_dst, fmt)

    @staticmethod
    def minus_minutes(date_str, num, fmt='%Y-%m-%d %H:%M:%S'):
        date = DateUtil.str2date(date_str)
        date_dst = date-datetime.timedelta(minutes=num)
        return DateUtil.date2str(date_dst, fmt)

    @staticmethod
    def plus_minutes(date_str, num, fmt='%Y-%m-%d %H:%M:%S'):
        return DateUtil.minus_minutes(date_str, -num, fmt)

    @staticmethod
    def now_date():
        return datetime.datetime.now()

    @staticmethod
    def yeaterday_str(fmt='%Y-%m-%d'):
        yesterday = DateUtil.now_date() - datetime.timedelta(days=1)
        return DateUtil.date2str(yesterday, fmt)

    @staticmethod
    def plus_days(date_str1, num, fmt='%Y-%m-%d'):
        date_dst = DateUtil.str2date(date_str1, fmt)+datetime.timedelta(days=num)
        return DateUtil.date2str(date_dst, fmt)

    @staticmethod
    def from_to(date_str_from, date_str_to, fmt='%Y-%m-%d'):
        days = DateUtil.minus_days(date_str_from, date_str_to, fmt)
        for i in range(days):
            yield DateUtil.plus_days(date_str_from, i, fmt)


if __name__ == "__main__":
    s = datetime.date(2017, 1, 1)
    print isinstance(s, datetime.date)
    s = '2017-1-1'
    start_date = (DateUtil.get_this_week_date(s))[0]
    print DateUtil.date2str(start_date, '%Y-%m-%d')