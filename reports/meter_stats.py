
import datetime
import sys, psycopg2
import calendar
from core.ommsdb import ommsDB


class meterStats(object):

   def __init__(self, db: ommsDB, dbid: int, dates: []):
      self.db: ommsDB = db
      self.meter_dbid = dbid
      self.dates: [] = dates
      self.dbtbl = "kwhrs"

   def init(self):
      pass

   def run(self, **kwargs):
      if "dbtable" in kwargs.keys():
         self.dbtbl = kwargs["dbtable"]
      outbuff: [] = []
      # -- -- -- for loop -- -- --
      for dt in self.dates:
         fst, lst = self.__read_fst_lst(dt)
         if fst is None or lst is None:
            print(f"MissingDataFor: {dt}")
            continue
         else:
            diff = round(float(lst[2]) - float(fst[2]), 2)
            __min: float = 30.0
            __max: float = 300.
            if __min < diff < __max:
               dt: datetime.date = dt
               d = f"{dt.year}/{dt.month:02d}/{dt.day:02d}"
               outbuff.append(f"{d}: {diff} kWh")
            else:
               print("BadData")
      # -- -- -- end of for loop -- -- --
      for s in outbuff:
         print(s)
      # -- -- -- -- -- --

   def __read_fst_lst(self, dt: str) -> ():
      qry = f"select t.fk_meter_dbid, t.reading_dts_utc, t.total_kwhrs from streams.kwhrs t " \
         f"where t.fk_meter_dbid = {self.meter_dbid} and date(t.reading_dts_utc) = '{dt}' " \
         f"order by t.reading_dts_utc asc;"
      arr: [] = self.db.qry_arr(qry)
      if arr is None or len(arr) == 0:
         return None, None
      # -- get fst & lst --
      fst, lst = arr[0], arr[-1]
      return fst, lst
