
import calendar as cal
import datetime, typing as t
# -- -- [ system ] -- --
from syscore.datatypes import *
from syscore.utils import utils
from syscore.ommsdb import ommsDB
from syscore.backfillcalc import backfillCalc


class meterMonthly(object):

   ERR_BACK_FILL = "UNABLE_TO_BACK_FILL"
   DB_TABLE_KWHRS = "streams.kwhs_raw"

   def __init__(self, db: ommsDB, met_circ_dbid: int, cirtag: str, y: int, m: int):
      self.db: ommsDB = db
      self.met_circ_dbid = met_circ_dbid
      self.cirtag = cirtag
      self.year: int = y
      self.month: int = m
      self.runid = None
      # self.tbl = "kwhrs_v2"
      self.tbl = "kwhs_raw"

   def init(self):
      assert self.db is not None

   def run(self, **kwargs):
      self.runid = kwargs["runid"]
      rval = self.__for_month(self.year, self.month)
      # -- -- -- --
      msg: str = (f"\n\t-- [ METER Monthly: {self.met_circ_dbid} | {self.cirtag} "
         f"| {self.year}/{self.month} | rval: {rval} ]")
      print(msg)
      # -- -- -- --
      if rval.startswith("-") and float(rval) < 0:
         print("= = = = = = = = = = = = = = = = = = = = = =\n")

   """
      this is a leftover from prev. code where month input was as an arr [month,..]
      will keep that for now 
   """
   def __for_month(self, y: int, m: int) -> str:
      try:
         #  get 1st & last reading this meter has in the db
         rcodeEnum, fst_read, lst_read = self.__get_fst_lst_readings(y, m)
         print([rcodeEnum, fst_read, lst_read])
         if rcodeEnum == kwhReadingsEnum.FIRST_LAST_BOTH_NONE:
            calc: backfillCalc =\
               backfillCalc(y=y, m=m, db=self.db, met_circ_dbid=self.met_circ_dbid, circ_tag=self.cirtag)
            calc.no_year_month_data_found()
         elif rcodeEnum == kwhReadingsEnum.FIRST_LAST_SAME:
            calc: backfillCalc =\
               backfillCalc(y=y, m=m, db=self.db, met_circ_dbid=self.met_circ_dbid, circ_tag=self.cirtag)
            calc.single_year_month_data_found()
         elif rcodeEnum == kwhReadingsEnum.FIRST_LAST_ON_MARK:
            pass
         else:
            pass
         return ""
      except Exception as e:
         print(e)
      finally:
         pass

   def __get_fst_lst_readings(self, y: int, m: int) -> t.Tuple[kwhReadingsEnum, t.Any, t.Any]:
      # -- dates: start & end --
      start_date = f"{y}-{m:02d}-{1:02d}"
      # -- end date is the next month's 1st day; new year issue
      end_date = utils.nxt_month_1st_str(y, m)
      # -- -- qry sql -- --
      qry: str = "select t.met_circ_dbid, t.dts_utc, t.total_kwhs, t.is_backfilled"\
         f" from {meterMonthly.DB_TABLE_KWHRS} t where t.met_circ_dbid = {self.met_circ_dbid}"\
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
      _kwhReadingsEnum: kwhReadingsEnum = kwhReadingsEnum.UNKNOWN
      # -- [ case: NONE readings found within the y, m for this met_circ_dbid ] --
      if fst_kwhrs is None and lst_kwhrs is None:
         _kwhReadingsEnum = kwhReadingsEnum.FIRST_LAST_BOTH_NONE
         return _kwhReadingsEnum, fst_kwhrs, lst_kwhrs
      # -- [ case: SINGLE reading found within y, m for this med_circ_dbid ] --
      if (fst_kwhrs is not None and lst_kwhrs is not None) and \
         (fst_kwhrs.reading_row_dbid == lst_kwhrs.reading_row_dbid):
         _kwhReadingsEnum = kwhReadingsEnum.FIRST_LAST_SAME
         return _kwhReadingsEnum, fst_kwhrs, lst_kwhrs
      # -- -- -- --
      _kwhReadingsEnum = kwhReadingsEnum.FIRST_LAST_ON_MARK
      return _kwhReadingsEnum, fst_kwhrs, lst_kwhrs

   def __read_top_row(self, rd: datetime.date, fst_lst: str) -> ():
      # -- map order by to get first or last row --
      if fst_lst not in ("fst", "lst"):
         raise Exception(f"BadFstLst: {fst_lst}")
      # -- run --
      _dir = {"fst": "asc", "lst": "desc"}[fst_lst]
      sdate = f"{rd.year}-{rd.month:02d}-{rd.day:02d}"
      edate = utils.next_month_day_str(rd.year, rd.month)
      # -- return qry --
      # t.fk_meter_dbid, t.dts_utc, t.total_kwhrs
      qry = f"select t.row_id, t.fk_meter_dbid, t.reading_dts_utc, t.total_kwhrs" \
         f" from streams.{self.tbl} t where t.fk_meter_dbid = {self.met_circ_dbid}" \
         f" and t.reading_dts_utc >= '{sdate}' and t.reading_dts_utc < '{edate}'" \
         f" order by t.reading_dts_utc {_dir} limit 1;"
      rval: () = self.db.query(qry)
      if rval is None or len(rval) == 0:
         return None
      else:
         return rval

   def __fst_reading(self, y: int, m: int) -> [(), None]:
      read_date: datetime.date = datetime.date(y, m, 1)
      tup_out = self.__read_top_row(read_date, "fst")
      if tup_out is not None:
         return tup_out
      # -- try backfill --
      # bf: backfillCalc = backfillCalc(self.db, self.tbl, self.met_circ_dbid)
      # if bf.backfill_fst_of_month(read_date):
      #    tup_out = self.__read_top_row(read_date, "fst")
      # else:
      #    dts_now = utils.dts_now()
      #    EMSG = f"{meterMonthly.ERR_BACK_FILL}_FST"
      #    self.__ins_qry(self.met_circ_dbid, y, m, 1, EMSG, 0, dts_now, 0, dts_now, 0)
      #    return None
      # -- return --
      return tup_out

   def __lst_reading(self, y: int, m: int) -> [(), None]:
      _, day_idx = cal.monthrange(y, m)
      read_date: datetime.date = datetime.date(y, m, day_idx)
      tup_out = self.__read_top_row(read_date, "lst")
      if tup_out is not None:
         return tup_out
      # -- try backfill --
      # bf: backfillCalc = backfillCalc(self.db, self.tbl, self.met_circ_dbid)
      # if bf.backfill_lst_of_month(read_date):
      #    tup_out = self.__read_top_row(read_date, "lst")
      # else:
      #    dts_now = utils.dts_now()
      #    EMSG = f"{meterMonthly.ERR_BACK_FILL}_LST"
      #    self.__ins_qry(self.met_circ_dbid, y, m, 1, EMSG, 0, dts_now, 0, dts_now, 0)
      #    return None
      # -- return --
      return tup_out

   # = = = = = = = = = = insert query string  = = = = = = = = = = = =
   def __ins_qry(self, dbid: int, y: int, m: int, err: int, errmsg: str
         , skwh: float, sdts: str, ekwh: float, edts: str, kwhdiff: float):
      try:
         qry: str = f"insert into reports.elec_meter_monthly" \
            f" values(default, {dbid}, '{self.runid}', {err}, '{errmsg}', {y}, {m}," \
            f" {skwh}, '{sdts}', {ekwh}, '{edts}', {kwhdiff}, now());"
         # -- run query --
         if self.db.insert_1row(qry) == 1:
            print("\t  + monthly report inserted")
         else:
            print("\t  - monthly report NOT inserted")
      except Exception as _e:
         print(_e)
      finally:
         pass


   def __try_backfill(self):
      pass
