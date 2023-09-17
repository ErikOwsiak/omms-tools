
import os.path
import xlsxwriter as xw
from xlsxwriter.workbook import Worksheet
from core.ommsops import ommsOps


class xlsout(object):

   def __init__(self, dbops: ommsOps, runid: str, y: int, m: int):
      self.dbops: ommsOps = dbops
      self.runid = runid
      self.year = y
      self.month = m
      self.wb: xw.workbook = None

   def create(self, outfld: str):
      # -- -- -- --
      if not os.path.exists(outfld):
         raise FileNotFoundError(outfld)
      # -- -- -- --
      file_path = f"{outfld}/omms_report_{self.year}_{self.month}.xlsx"
      arr_arr: [] = self.dbops.get_runid_clt_data(self.runid)
      self.wb: xw.workbook = xw.Workbook(file_path)
      wsh_totals = self.wb.add_worksheet("Clients")
      rval = self.__fill_clt_wsh(arr_arr, self.year, self.month, wsh_totals)
      # -- nxt --
      wsh_meters = self.wb.add_worksheet("Meters")
      rval = self.__fill_meter_wsh(arr_arr, wsh_meters)
      # -- nxt --
      wsh_info = self.wb.add_worksheet("LookupInfo")
      info_tbl: [] = self.dbops.get_info()
      self.__fill_lookup_info(info_tbl, wsh_info)
      # -- close & save file --
      self.wb.close()

   def __fill_lookup_info(self, arr_tups: [], wsh: Worksheet) -> bool:
      # - - - - - -
      row_idx = 0
      wsh.set_column(0, 0, 16)
      wsh.write(row_idx, 0, "NIP")
      wsh.write(row_idx, 1, "space_tag")
      wsh.set_column(2, 2, 16)
      wsh.write(row_idx, 2, "circuit_tag")
      # --- fill ---
      col0_w: int = 0
      for tup in arr_tups:
         row_idx += 1
         nip, clt, ctag = tup
         wsh.write(row_idx, 0, nip)
         col0_w = col0_w if col0_w > len(clt) else (len(clt) + 8)
         wsh.set_column(1, 1, col0_w)
         wsh.write(row_idx, 1, clt)
         wsh.write(row_idx, 2, ctag)
      return True

   def __fill_meter_wsh(self, arr_tups: [], wsh_meters: Worksheet) -> bool:
      dct = {"bold": True, "font_color": "#091378", "font_size": 12}
      bold_cell = self.wb.add_format(dct)
      # -- load data --
      col0_w: int = 0
      row_idx: int = 0
      for tup in arr_tups:
         row_idx += 1
         wsh_meters.write(row_idx, 0, "")
         nip, clt, y, m, tkwh, calc = tup
         row_idx += 1
         tag_cld = f"{nip} | {clt}"
         col0_w = col0_w if col0_w > len(tag_cld) else (len(tag_cld) + 8)
         wsh_meters.set_column(0, 0, col0_w)
         wsh_meters.write(row_idx, 0, tag_cld, bold_cell)
         calcs: [] = calc.split("|")
         # -- for each calc --
         for c in calcs:
            mid, cir, kwh, lst = c.strip().split(";")
            msg: str = f"        {cir}:  {kwh} kwh"
            row_idx += 1
            wsh_meters.write(row_idx, 0, msg)
      # -- the end --
      return True

   def __fill_clt_wsh(self, arr_arr: [], y: int, m: int, wsh_totals: Worksheet) -> bool:
      # -- col names --
      wsh_totals.write(0, 0, "NIP")
      wsh_totals.write(0, 1, "Client_Name")
      wsh_totals.write(0, 2, f"{y}_{m}_kWh")
      ln_idx: int = 1
      # -- cell format --
      fnt_color = "#800000"
      dct = {"bold": True, "font_color": f"{fnt_color}", "font_size": 12}
      clt_frmt = self.wb.add_format(dct)
      # -- load data --
      col0_w = 0
      col1_w = 0
      for arr in arr_arr:
         col0_w = col0_w if col0_w > len(arr[0]) else (len(arr[0]) + 8)
         wsh_totals.set_column(0, 0, col0_w)
         wsh_totals.write(ln_idx, 0, arr[0])
         # -- col width --
         col1_w = col1_w if col1_w > len(arr[1]) else (len(arr[1]) + 8)
         wsh_totals.set_column(1, 1, col1_w)
         wsh_totals.write(ln_idx, 1, arr[1], clt_frmt)
         # -- -- -- -- -- --
         # -- date: year-month --
         wsh_totals.set_column(2, 2, 14)
         wsh_totals.write(ln_idx, 2, round(float(arr[4]), 2))
         ln_idx += 1
      # -- the end --
      return True
