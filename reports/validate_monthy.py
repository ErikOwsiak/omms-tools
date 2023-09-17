
import typing as t
# -- [ system ] --
from syscore.datatypes import *
from syscore.utils import utils
from syscore.ommsdb import ommsDB


class validateMonthly(object):

   def __init__(self, db: ommsDB, met_circ_dbid: int, cirtag: str, y: int, m: int):
      self.db: ommsDB = db
      self.met_circ_dbid = met_circ_dbid
      self.cirtag = cirtag
      self.year: int = y
      self.month: int = m
      self.runid = None

   def init(self):
      assert self.db is not None

   def run(self) -> t.Tuple[kwhReading, kwhReading]:
      hdr: str = "Validating METER Monthly"
      msg: str = f"\n\t-- [ {hdr}: {self.met_circ_dbid} | {self.cirtag} " \
         f"| {self.year}/{self.month} ]"
      print(msg)
      fst_read, lst_read = self.__run(self.year, self.month)
      print(f"\t\tfound: {fst_read} | {lst_read}")
      return fst_read, lst_read

   """
      this is a leftover from prev. code where month input was as an arr [month,..]
      will keep that for now 
   """
   def __run(self, y: int, m: int) -> t.Tuple[kwhReading, kwhReading]:
      try:
         return self.__get_fst_lst_readings(y, m)
      except Exception as e:
         print(e)
      finally:
         pass

   def __get_fst_lst_readings(self, y: int, m: int) -> t.Tuple[kwhReading, kwhReading]:
      # -- dates: start & end --
      start_date = f"{y}-{m:02d}-{1:02d}"
      # -- end date is the next month's 1st day; new year issue
      end_date = utils.nxt_month_1st_str(y, m)
      # -- -- qry sql -- --
      tbl_kwhrs = ommsDBTables.KWHRS_READINGS
      qry: str = "select t.row_dbid, t.met_circ_dbid, t.dts_utc, t.total_kwhs, t.is_backfilled"\
         f" from {tbl_kwhrs} t where t.met_circ_dbid = {self.met_circ_dbid}"\
         f" and t.dts_utc >= '{start_date}' and t.dts_utc < '{end_date}'"\
         f" order by t.dts_utc %s limit 1;"
      # -- -- [ 1st ] -- --
      day_fst_qry = qry % "asc"
      day_fst_qry_rsts: () = self.db.query(day_fst_qry)
      fst_kwhrs: kwhReading = kwhReading.getInstance(day_fst_qry_rsts)
      # -- -- [ last ] -- --
      day_lst_qry = qry % "desc"
      day_lst_qry_rsts: () = self.db.query(day_lst_qry)
      lst_kwhrs: kwhReading = kwhReading.getInstance(day_lst_qry_rsts)
      # -- -- -- --
      return fst_kwhrs, lst_kwhrs
