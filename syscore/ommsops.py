
import datetime, calendar as cal
from syscore.ommsdb import ommsDB


class ommsOps(object):

   def __init__(self, db: ommsDB):
      self.db: ommsDB = db

   def get_electric_met_circ_info(self) -> []:
      qry = "select t.met_cir_rowid, t.met_syspath, t.cir_tag from core.elec_meter_circuits t;"
      return self.db.qry_arr(qry)

   def get_clients(self) -> []:
      qry = "select * from reports.clients;"
      return self.db.qry_arr(qry)

   def get_client_meters(self, clt_id: int, clt_tag: str):
      qry = "select * from "

   def read_meter_dbid(self, cir_tag: str) -> int:
      qry = f"select t.meter_dbid from config.meters t where t.circuit_tag = '{cir_tag}';"
      obj = self.db.qry_val(qry)
      return int(obj)

   def read_circuit_tag(self, meter_id: int) -> str:
      qry = f"select t.circuit_tag from config.meters t where t.meter_dbid = {meter_id};"
      obj = self.db.qry_val(qry)
      return str(obj)

   def fix_meter_reading(self, m_dbid: int, year: int, month: int, s_kwh: float, e_kwh: float):
      # get prev month:year, next month:year
      diff_1day = datetime.timedelta(days=1)
      this_month_fst = datetime.date(year, month, 1)
      prev_month_lst = this_month_fst - diff_1day
      this_month_days = cal.monthrange(year, month)[1]
      this_month_lst = datetime.date(year, month, this_month_days)
      next_month_fst = this_month_lst + diff_1day
      # -- insert into last hr of  last day of prev month --
      tbl = "kwhrs_v1"
      d: datetime.date = prev_month_lst
      dts_utc = f"{d.year}-{d.month}-{d.day} 23:55:55"
      kwh_996 = round(s_kwh * 0.996, 2)
      ins0 = f"insert into streams.{tbl} values(default, {m_dbid}, " \
         f"'{dts_utc}', 0.22, {kwh_996}, 0, 0, 0, now());"
      db.insert_1row(ins0)
      # -- insert into the first of the month --
      d: datetime.date = this_month_fst
      dts_utc = f"{d.year}-{d.month}-{d.day} 00:05:55"
      ins1 = f"insert into streams.{tbl} values(default, {m_dbid}, " \
         f"'{dts_utc}', 0.22, {s_kwh}, 0, 0, 0, now());"
      db.insert_1row(ins1)
      # -- insert into the last hour of the last day of the month --
      d: datetime.date = this_month_lst
      dts_utc = f"{d.year}-{d.month}-{d.day} 23:55:55"
      ins2 = f"insert into streams.{tbl} values(default, {m_dbid}, " \
         f"'{dts_utc}', 0.22, {e_kwh}, 0, 0, 0, now());"
      db.insert_1row(ins2)
      # -- insert into the first hr; 1st day of next month --
      d: datetime.date = next_month_fst
      dts_utc = f"{d.year}-{d.month}-{d.day} 00:05:55"
      kwh_106 = (e_kwh * 1.06)
      ins3 = f"insert into streams.{tbl} values(default, {m_dbid}, " \
             f"'{dts_utc}', 0.22, {kwh_106}, 0, 0, 0, now());"
      db.insert_1row(ins3)

   def get_runid_clt_data(self, runid: str) -> []:
      qry = f"select t.clt_tag, t.clt_name , t.\"year\", t.\"month\", t.total_kwh, t.calc_notes" \
         f" from reports.elec_client_monthly t where run_id = '{runid}';"
      return self.db.qry_arr(qry)

   def get_info(self):
      qry = f"select t.client_tag, t.space_tag, t.circuit_tag from" \
         f" reports.client_space_circuits t;"
      return self.db.qry_arr(qry)


# -- test --
if __name__ == "__main__":
   pass
   # db: ommsDB = ommsDB()
   # if db.connect():
   #    ops: ommsOps = ommsOps(db)
   #    s_kwhrs = 2352.06
   #    # ops.fix_meter_reading(1077, 2022, 10, s_kwhrs, (s_kwhrs + 515.3))
