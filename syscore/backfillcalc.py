
import datetime
import typing as t
import calendar as cal
# -- -- [ system ] -- --
from syscore.datatypes import *
from syscore.utils import utils
from syscore.ommsdb import ommsDB
from syscore.datatypes import kwhReading


class backfillCalc(object):

   def __init__(self, y: int, m: int, met_circ_dbid: int, circ_tag: str
         , db: ommsDB, fst_read: kwhReading, lst_read: kwhReading):
      # -- -- -- -- -- --
      self.year: int = y
      self.month: int = m
      self.ommsdb: ommsDB = db
      self.circ_tag: str = circ_tag
      self.met_circ_dbid: int = met_circ_dbid
      self.fst_read: kwhReading = fst_read
      self.lst_read: kwhReading = lst_read

   """
      reading should be "some reading" within the month but could be null; 
         -> if null: search full span of readings from prev <<< and next >>> readings
            for this met_circ_dbid
         1. if reading is not None then just search for 1st prev reading 
            from prev month
   """
   # def first_of_month(self, reading: [None, kwhReading]):
   #    if reading is None:
   #       self.__load_met_circ_prev_next_datapoints()

   def try_fill_missing_readings(self) -> ():
      try:
         # -- not data for report month --
         print("\n\n[ + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + + ]")
         if self.fst_read is None and self.lst_read is None:
            fst_kwhrs, lst_kwhrs = self.__no_datapoints()
            print(f"\t[ {self.met_circ_dbid} | fst/ {fst_kwhrs} lst/{lst_kwhrs} ]")
            print("[ - - - - - - - - - - - - - - - - - - - - - - - - ]")
            return fst_kwhrs, lst_kwhrs
         # -- single data point for the month --
         if self.fst_read.read_row_dbid == self.lst_read.read_row_dbid:
            self.__single_datapoint()
            return None, None
         # -- datapoints inside of report month
         return None, None
      except Exception as e:
         print(e)
         return None, None
      finally:
         pass

   def __no_datapoints(self) -> t.Tuple[float, float]:
      try:
         print(f"[ {self.met_circ_dbid}/{self.circ_tag} | __no_year_month_datapoints ]")
         fst_prev, fst_post = self.__load_met_circ_prev_next_datapoints()
         print(f"{fst_prev} :: {fst_post}")
         assert fst_prev is not None and fst_post is not None
         # -- -- -- --
         if not (fst_post.dts_utc > fst_prev.dts_utc):
            raise ValueError("PostReadingIsGreaterThanPrevReading")
         # -- -- -- --
         dt_delta: dt.timedelta = fst_post.dts_utc - fst_prev.dts_utc
         kw_delta: float = round((fst_post.total_kwhrs - fst_prev.total_kwhrs), 2)
         daily: float = round((kw_delta / dt_delta.days), 2)
         print([dt_delta.days, kw_delta, daily])
         # check if fst_prev is on the last day of prev month
         # if so just promote to 1st reading
         #   + extra 0.1% increase in kwh
         minutes_in_day: int = 60 * 24
         fst_kwhrs: float = 0.0; lst_kwhrs: float = 0.0; month_kwhrs = 0.0
         if (fst_prev.dts_utc.month == (self.month - 1)) and fst_prev.is_lst_of_month():
            kwh_tick: float = round((daily / minutes_in_day), 4)
            fst_kwhrs = round((fst_prev.total_kwhrs + kwh_tick), 2)
         else:
            pass
         # -- -- -- --
         # calc lst_kwhrs should be daily * number of days
         # in the report month
         rpt_month_days: int = cal.monthrange(self.year, self.month)[1]
         rpt_last_dts = dt.datetime(self.year, self.month, rpt_month_days, 23, 58, 58)
         # -- -- -- --
         if not (fst_post.dts_utc > rpt_last_dts):
            raise ValueError("PostReadingIsNotGreaterThanReportLastDayOfMonth")
         # -- -- -- --
         month_kwhrs = round((daily * rpt_month_days), 2)
         lst_kwhrs = round((fst_kwhrs + month_kwhrs), 2)
         if lst_kwhrs > fst_post.total_kwhrs:
            raise ValueError(f"BadCalc: lst_kwhrs/ {lst_kwhrs}")
         print(f"CalcResults: met_circ/{self.met_circ_dbid} fst/{fst_kwhrs} lst/{lst_kwhrs} month/{month_kwhrs}")
         return fst_kwhrs, lst_kwhrs
      except Exception as e:
         print(e); raise e
      finally:
         pass

   # def single_year_month_data_found(self):
   #    print(f"[ {self.met_circ_dbid}/{self.circ_tag} | single_year_month_data_found ]")

   def __fst_previous_reading(self, dts: str) -> [(), None]:
      qry = f"select t.row_dbid, t.met_circ_dbid, t.dts_utc, t.total_kwhs, t.is_backfilled"\
         f" from streams.kwhs_raw t where t.met_circ_dbid = {self.met_circ_dbid} and "\
         f" t.dts_utc < '{dts}' order by t.dts_utc desc limit 1;"
      row: () = self.ommsdb.qry_row(qry)
      return row

   def __fst_next_reading(self, dts: str) -> [(), None]:
      qry = f"""select t.row_dbid, t.met_circ_dbid, t.dts_utc, t.total_kwhs, t.is_backfilled
         from streams.kwhs_raw t where t.met_circ_dbid = {self.met_circ_dbid}
         and t.dts_utc > '{dts}' order by t.dts_utc asc limit 1;"""
      row: () = self.ommsdb.qry_row(qry)
      return row

   # def backfill_fst_of_month(self, dt: datetime.date):
   #    # -- -- -- --
   #    print(f"\n\t[ backfill_fst_of_month: {dt.year}/{dt.month}/{dt.day} ]")
   #    # -- -- -- --
   #    __fst_pre_tup = self.__fst_previous_reading(dt)
   #    if __fst_pre_tup is None:
   #       return False
   #    # -- -- -- --
   #    __fst_nxt_tup = self.__fst_next_reading(dt)
   #    if __fst_nxt_tup is None:
   #       return False
   #    # -- needed info --
   #    print(["__fst_pre_tup", __fst_pre_tup])
   #    print(["__fst_nxt_tup", __fst_nxt_tup])
   #    # -- do --
   #    p_dbid, p_dts, p_tkwhrs = __fst_pre_tup
   #    n_dbid, n_dts, n_tkwhrs = __fst_nxt_tup
   #    # -- get # of days --
   #    tdt: datetime.timedelta = (n_dts - p_dts)
   #    daily: float = round(((n_tkwhrs - p_tkwhrs) / tdt.days), 6)
   #    # -- days from rst reading to the 1st of current month --
   #    p_dts: datetime.datetime = p_dts
   #    xdt: datetime.timedelta = (dt - p_dts.date())
   #    t_kwhrs: float = round(((daily * xdt.days) + p_tkwhrs), 4)
   #    # -- insert on the last day of prev month --
   #    pdt: datetime.date = dt - datetime.timedelta(days=-1)
   #    dts = f"{pdt.year}-{pdt.month:02d}-{pdt.day:02d} 23:59:52"
   #    qry = f"insert into streams.{self.tbl}" \
   #          f" values (default, {p_dbid}, true, '{dts}', 0.22, {t_kwhrs}, 0,0,0, now());"
   #    # -- insert on the 1st day of the current month --
   #    v0 = self.ommsdb.insert_1row(qry)
   #    dts = f"{dt.year}-{dt.month:02d}-01 00:01:01"
   #    qry = f"insert into streams.{self.tbl}" \
   #       f" values (default, {p_dbid}, true, '{dts}', 0.22, {t_kwhrs}, 0, 0, 0, now());"
   #    v1 = self.ommsdb.insert_1row(qry)
   #    # -- the end --
   #    return True
   #
   # def backfill_lst_of_month(self, dt: datetime.date) -> bool:
   #    # -- -- -- --
   #    print(f"\n\t[ backfill_lst_of_month: {dt.year}/{dt.month}/{dt.day} ]")
   #    # -- -- -- --
   #    __fst_pre_tup = self.__fst_previous_reading(dt)
   #    if __fst_pre_tup is None:
   #       return False
   #    # -- -- -- --
   #    __fst_nxt_tup = self.__fst_next_reading(dt)
   #    if __fst_nxt_tup is None:
   #       return False
   #    # -- needed info --
   #    print(["__fst_pre_tup", __fst_pre_tup])
   #    print(["__fst_nxt_tup", __fst_nxt_tup])
   #    # -- do --
   #    p_dbid, p_dts, p_tkwhrs = __fst_pre_tup
   #    n_dbid, n_dts, n_tkwhrs = __fst_nxt_tup
   #    # -- get # of days --
   #    tdt: datetime.timedelta = (n_dts - p_dts)
   #    daily: float = round(((n_tkwhrs - p_tkwhrs) / tdt.days), 6)
   #    _, _days = cal.monthrange(dt.year, dt.month)
   #    t_kwhrs: float = round(((daily * _days) + p_tkwhrs), 4)
   #    # -- insert on the lst day of the current month --
   #    dts = f"{dt.year}-{dt.month:02d}-{dt.day:02d} 23:59:52"
   #    qry = f"insert into streams.{self.tbl}" \
   #       f" values (default, {p_dbid}, true, '{dts}', 0.22, {t_kwhrs}, 0, 0, 0, now());"
   #    v0 = self.ommsdb.insert_1row(qry)
   #    pdt: datetime.date = dt + datetime.timedelta(days=1)
   #    dts = f"{pdt.year}-{pdt.month:02d}-{pdt.day:02d} 00:01:01"
   #    qry = f"insert into streams.{self.tbl}" \
   #       f" values (default, {p_dbid}, true, '{dts}', 0.22, {t_kwhrs}, 0, 0, 0, now());"
   #    v1 = self.ommsdb.insert_1row(qry)
   #    # -- the end --
   #    return True

   def __load_met_circ_prev_next_datapoints(self) -> t.Tuple[kwhReading, kwhReading]:
      # -- -- -- --
      fst_of_month = utils.fst_datetime_of_month(self.year, self.month)
      data_row = self.__fst_previous_reading(fst_of_month)
      fst_kwh: kwhReading = kwhReading.getInstance(data_row)
      # -- -- -- --
      lst_of_month = utils.lst_datetime_of_month(self.year, self.month)
      data_row = self.__fst_next_reading(lst_of_month)
      lst_kwh: kwhReading = kwhReading.getInstance(data_row)
      # -- -- -- --
      return fst_kwh, lst_kwh

   def __single_datapoint(self):
      print("[ __single_datapoint ]")
