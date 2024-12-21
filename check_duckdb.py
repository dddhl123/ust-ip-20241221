import duckdb
from tqdm import tqdm
con = duckdb.connect("test.db")
q7 = """
COPY (
    select n1.n_name,
           n2.n_name,
           l_shipdate,
           sum(l_extendedprice * (1 - l_discount)) as volume,
           count(*) AS cnt
    from supplier s,
         lineitem l,
         orders o,
         customer c,
         nation n1,
         nation n2
    where s.s_suppkey = l.l_suppkey
      and o.o_orderkey = l.l_orderkey
      and c.c_custkey = o.o_custkey
      and s.s_nationkey = n1.n_nationkey
      and c.c_nationkey = n2.n_nationkey
      and (n1.n_name = 'FRANCE' or n1.n_name = 'GERMANY')
      and (n2.n_name = 'FRANCE' or n2.n_name = 'GERMANY')
      and n1.n_name <> n2.n_name
      and l.l_shipdate >= date '1995-01-01'
      and l.l_shipdate <= date '1996-12-31'
    group by n1.n_name,
             n2.n_name,
             l.l_shipdate
) TO 'test2.result' WITH (FORMAT CSV, HEADER TRUE);
"""
print(con.sql(q7))