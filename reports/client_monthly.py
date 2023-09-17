
from core.ommsops import ommsOps
from core.utils import utils


class clientMonthly(object):

   def __init__(self, ops: ommsOps, dbid: int, name: str, tag: str):
      self.ommsops = ops
      self.dbid = dbid
      self.clt_tag = tag
      self.name = name

   def int(self):
      pass

   def run(self, **kwargs):
      runid = kwargs["runid"]
      year = kwargs["year"]
      month = kwargs["month"]
      data: [] = self.__read_data(year, month, runid)
      return self.__process_data(runid, year, month, data)

   def __read_data(self, y: int, m: int, runid: str) -> []:
      # ---
      qry = "select emm.\"year\", emm.\"month\", emm.fk_meter_dbid, m.circuit_tag," \
         " emm.end_start_diff_kwh, emm.end_read_kwh, emm.end_read_dts" \
         " from config.MET_CIRC_ARR m join reports.elec_meter_monthly emm on m.met_circ_dbid = emm.fk_meter_dbid" \
         " join reports.client_space_circuits csc on m.circuit_tag = csc.circuit_tag" \
         f" where csc.client_tag = '{self.clt_tag}' and emm.\"year\" = {y} and emm.\"month\" = {m}" \
         f" and emm.run_id = '{runid}';"
      # ---
      rows: [] = self.ommsops.db.qry_arr(qry)
      return rows

   def __process_data(self, runid: str, y: int, m: int, data: []) -> ():
      reps: [] = []; total_kwh: float = 0.0
      for item in data:
         _, _, dbid, cir_id, diff_kwh, last_read_kwh, _ = item
         total_kwh += float(diff_kwh)
         reps.append(f"{dbid};{cir_id};{diff_kwh};{last_read_kwh}")
      # -- do --
      calc_notes = " | ".join(reps)
      qry = f"insert into reports.elec_client_monthly values(default, {self.dbid}, '{runid}'," \
         f" '{self.clt_tag}', '{self.name}', {y}, {m}, {total_kwh}, '{calc_notes}', now());"
      if self.ommsops.db.insert_1row(qry) == 1:
         return round(total_kwh, 2), calc_notes
      else:
         return -1.0, None
