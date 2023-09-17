#!/usr/bin/env python3

import psycopg2
from psycopg2.extensions import cursor


class ommsDB(object):

   def __init__(self, conn_str):
      self.conn_str: str = conn_str
      self.dbconn = None

   def connect(self) -> bool:
      OPEN: int = 1
      assert self.conn_str not in [None, ""]
      self.dbconn = psycopg2.connect(self.conn_str)
      if self.dbconn is not None and self.dbconn.status == OPEN:
         print("\tConnected")
         return True
      else:
         print("\tNotConnected")
         return False

   def dbconn(self):
      return self.dbconn

   def query(self, qry) -> [None, ()]:
      cur: cursor = None
      try:
         cur = self.dbconn.cursor()
         cur.execute(qry)
         tmp = cur.fetchone()
         return tmp
      except Exception as e:
         print(e)
      finally:
         cur.close()

   def qry_1st_last(self, qry: str) -> []:
      cur = None
      try:
         arr_out = []
         cur = self.dbconn.cursor()
         cur.execute(qry)
         for rec in cur:
            arr_out.append(rec)
         return arr_out
      except Exception as e:
         print(e)
         return None
      finally:
         cur.close()

   def qry_first_last_rows(self, qry: str) -> [(), ()]:
      cur: cursor = None
      try:
         arr_out = []
         cur: cursor = self.dbconn.cursor()
         cur.execute(qry)
         for rec in cur:
            arr_out.append(rec)
         return arr_out
      except Exception as e:
         print(e)
         return None
      finally:
         cur.close()

   def qry_row(self, qry: str) -> ():
      cur: cursor = None
      try:
         cur: cursor = self.dbconn.cursor()
         cur.execute(qry)
         return cur.fetchone()
      except Exception as e:
         print(e)
         return None
      finally:
         cur.close()

   def qry_arr(self, qry: str) -> []:
      cur: cursor = None
      try:
         arr_out = []
         cur: cursor = self.dbconn.cursor()
         cur.execute(qry)
         for rec in cur:
            arr_out.append(rec)
         return arr_out
      except Exception as e:
         print(e)
         return None
      finally:
         cur.close()

   def qry_val(self, qry: str) -> object:
      arr = self.qry_arr(qry)
      val = arr[0][0]
      return val

   def insert_1row(self, qry: str) -> int:
      cur = None
      try:
         cur = self.dbconn.cursor()
         cur.execute(qry)
         self.dbconn.commit()
         return cur.rowcount
      except Exception as e:
         print(e)
         return -1
      finally:
         cur.close()

   # def backfill_reading(self, dbid: int, dts_str: str, tkwhrs: float):
   #    qry = f"insert into streams.kwhs_raw"\
   #       f" values (default, {dbid}, true, '{dts_str}', 0.02, {tkwhrs}, 0, 0, 0, now());"
