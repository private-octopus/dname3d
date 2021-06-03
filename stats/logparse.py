#!/usr/bin/python
# coding=utf-8
#
# Parse the log file, build a log file object
import datetime
import traceback

class TZ0(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours=0)
    def dst(self, dt):
        return datetime.timedelta(0)
    def tzname(self,dt):
        return "+00:00"
    def  __repr__(self):
        return f"{self.__class__.__name__}()"

class logline:
    def __init__(self):
        self.h = ""
        self.date = datetime.datetime(year=2000,month=1,day=1)
        self.utcdate = datetime.date(year=2000,month=1,day=1)
        self.count = 0
    def from_csv(self, t):
        ret = False
        p = t.split(",")
        if len(p) >= 1 and len(p[0]) > 8:
            self.h = p[0]
            if len(p) >= 2:
                try:
                    self.date = datetime.datetime.fromisoformat(p[1])
                    tzu = TZ0(0)
                    utcdt = self.date.astimezone(tzu)
                    self.utcdate = utcdt.date()
                    #self.utcdate = self.date.date()
                    if len(p) > 3:
                        try:
                            self.count = int(p[3])
                        except:
                            self.count = 0
                    ret = True
                except Exception as e:
                    traceback.print_exc()
                    print("Cannot parse date <" + p[1] + ">\nException: " + str(e))
                    self.date = datetime.datetime(year=2000,month=1,day=1)
        return ret
    def to_csv(self):
        t = self.h + "," + self.date.isoformat() + "," + self.utcdate.isoformat() + "," + str(self.count) + "\n"
        return t
    def csv_head():
        return("commit, date, utcdate, count\n")

class logfile:
    def __init__(self):
        self.list = dict()
    def add_line(self, line):
        ll = logline()
        if ll.from_csv(line):
            self.list[ll.h] = ll
    def load_file(self, logfile):
        for line in open(logfile , "rt"):
            self.add_line(line)
    def diff_line(self, line, oldfile):
        ll = logline()
        if ll.from_csv(line):
            if not ll.h in oldfile.list:
                self.list[ll.h] = ll
    def diff_file(self, logfile, oldfile):
        for line in open(logfile , "rt"):
            self.diff_line(line, oldfile)
    def write_file(self,logfile):
        f = open(logfile , "wt", encoding="utf-8")
        f.write(logline.csv_head())
        for h in self.list:
            f.write(self.list[h].to_csv())
        f.close()
    def append_line(logfile, ll):
        f = open(logfile,"at")
        f.write(ll.to_csv())
        f.close()




