
import enum
import datetime as dt
import calendar as cal


class ommsDBTables(object):
   KWHRS_READINGS: str = "streams.kwhs_raw"


class kwhReadingsEnum(enum.IntFlag):
   FIRST_LAST_ON_MARK = 1000
   FIRST_LAST_BOTH_NONE = 1002
   FIRST_LAST_SAME = 1004
   FIRST_NONE = 1006
   LAST_NONE = 1008
   UNKNOWN = 1100


"""
   select t.row_dbid, t.met_circ_dbid, t.dts_utc
      , t.total_kwhs, t.is_backfilled from streams.kwhs_raw
"""
class kwhReading(object):

   @staticmethod
   def getInstance(row: ()):
      if row is None:
         return None
      row_dbid, met_circ_dbid, dts_utc, total_kwhs, is_backfill = row
      return kwhReading(row_dbid, met_circ_dbid, dts_utc, total_kwhs, is_backfill)

   def __init__(self, read_row_dbid: int, met_cir_dbid: int
         , dtsutc: dt.datetime, total_kwhrs: float, is_backfill: bool):
      # -- -- -- -- -- -- -- --
      self.read_row_dbid: int = read_row_dbid
      self.met_cir_dbid: int = met_cir_dbid
      self.dts_utc: dt.datetime = dtsutc
      self.total_kwhrs: float = total_kwhrs
      self.is_backfill: bool = is_backfill
      # -- -- -- -- -- -- -- --

   def __str__(self):
      return f"[ {self.met_cir_dbid} | {self.dts_utc} | {self.total_kwhrs} kWhrs]"

   def is_fst_of_month(self) -> bool:
      return self.dts_utc.day == 1

   def is_lst_of_month(self) -> bool:
      dn, days = cal.monthrange(self.dts_utc.year, self.dts_utc.month)
      return self.dts_utc.day == days
