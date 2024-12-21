import duckdb
from tqdm import tqdm

con = duckdb.connect("test.db")

c1 = "CREATE OR REPLACE TABLE customer (c_custkey    BIGINT NOT NULL,c_name VARCHAR NOT NULL,c_address VARCHAR NOT NULL,c_nationkey  INT NOT NULL,c_phone VARCHAR NOT NULL,c_acctbal DECIMAL(15, 2) NOT NULL,c_mktsegment VARCHAR NOT NULL,c_comment VARCHAR NOT NULL);"
c2 = "CREATE OR REPLACE TABLE lineitem (l_orderkey  BIGINT NOT NULL,l_partkey        BIGINT NOT NULL,l_suppkey        BIGINT NOT NULL,l_linenumber     BIGINT NOT NULL,l_quantity       DECIMAL(15, 2) NOT NULL,l_extendedprice  DECIMAL(15, 2) NOT NULL,l_discount       DECIMAL(15, 2) NOT NULL,l_tax            DECIMAL(15, 2) NOT NULL,l_returnflag     VARCHAR NOT NULL,l_linestatus     VARCHAR NOT NULL,l_shipdate       DATE NOT NULL,l_commitdate     DATE NOT NULL,l_receiptdate    DATE NOT NULL,l_shipinstruct   VARCHAR NOT NULL,l_shipmode       VARCHAR NOT NULL,l_comment        VARCHAR NOT NULL);"
c3 = "CREATE OR REPLACE TABLE nation (n_nationkey INT NOT NULL,n_name      VARCHAR NOT NULL,n_regionkey INT NOT NULL,n_comment   VARCHAR);"
c4 = "CREATE OR REPLACE TABLE orders (o_orderkey   BIGINT NOT NULL,o_custkey       BIGINT NOT NULL,o_orderstatus   VARCHAR NOT NULL,o_totalprice    DECIMAL(15, 2) NOT NULL,o_orderdate     DATE NOT NULL,o_orderpriority VARCHAR NOT NULL,o_clerk         VARCHAR NOT NULL,o_shippriority  INT NOT NULL,o_comment       VARCHAR NOT NULL);"
c5 = "CREATE OR REPLACE TABLE supplier (s_suppkey BIGINT NOT NULL,s_name      VARCHAR NOT NULL,s_address   VARCHAR NOT NULL,s_nationkey INT NOT NULL,s_phone     VARCHAR NOT NULL,s_acctbal   DECIMAL(15, 2) NOT NULL,s_comment   VARCHAR NOT NULL);"

con.sql(c1)
con.sql(c2)
con.sql(c3)
con.sql(c4)
con.sql(c5)
print(con.sql("show tables;"))

rows = []
cnt = 0
with open('./large_test/test3.tbl', 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        parts = line.split('|')
        rows.append(parts)

for row in tqdm(rows):
    operation = row[0]
    table_name = row[1]
    fields = row[2:]
    concat = ""
    for field in fields:
        if field == "":
            continue
        concat = concat +  "'" +  str(field) + "',"
    if operation == "INSERT":
        insert_sql =  "INSERT INTO " + table_name + " VALUES (" + concat + ");"
        con.sql(insert_sql)
    elif operation == "DELETE":
        if table_name == "customer":
            delete_sql = f"DELETE FROM {table_name} WHERE c_custkey = {fields[0]};"
        elif table_name == "lineitem":
            delete_sql = f"DELETE FROM {table_name} WHERE l_orderkey = {fields[0]} AND l_partkey = {fields[1]};"
        elif table_name == "nation":
            delete_sql = f"DELETE FROM {table_name} WHERE n_nationkey = {fields[0]};"
        elif table_name == "orders":
            delete_sql = f"DELETE FROM {table_name} WHERE o_orderkey = {fields[0]};"
        elif table_name == "supplier":
            delete_sql = f"DELETE FROM {table_name} WHERE s_suppkey = {fields[0]};"
        print(delete_sql)
        con.sql(delete_sql)

print(con.sql("select count() from customer;"))
print(con.sql("select count() from lineitem;"))
print(con.sql("select count() from nation;"))
print(con.sql("select count() from orders;"))
print(con.sql("select count() from supplier;"))
con.close()