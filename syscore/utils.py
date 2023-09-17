
import calendar as cal
import datetime, time


class utils(object):

   @staticmethod
   def next_month_day_date(y: int, m: int, d: int = 1) -> datetime.date:
      if m == 12:
         y += 1
         m = 1
      # -- return date --
      return datetime.date(y, m, d)

   @staticmethod
   def next_month_day_str(y: int, m: int, d: int = 1) -> str:
      if m == 12:
         y += 1; m = 1
      else:
         m += 1
      # -- return date --
      return f"{y}-{m:02d}-{d:02d}"

   @staticmethod
   def nxt_month_1st_str(y: int, m: int):
      m = (m + 1) if m < 12 else 1
      return f"{y}-{m:02d}-{1:02d}"

   @staticmethod
   def dts_now():
      d = datetime.datetime.utcnow()
      return ("{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}"
         .format(d.year, d.month, d.day, d.hour, d.minute, d.second))

   @staticmethod
   def get_run_id():
      tme = int(time.time())
      return f"0x{tme:08x}"

   @staticmethod
   def year_month_days(y: int, m: int):
      return cal.monthrange(y, m)[1]

   @staticmethod
   def fst_datetime_of_month(y, m):
      return "{:04}-{:02}-01 00:02:02".format(y, m)

   @staticmethod
   def lst_datetime_of_month(y, m):
      mr: cal.monthrange = cal.monthrange(y, m)
      _, days = mr
      return "{:04}-{:02}-{:02} 23:58:58".format(y, m, days)
