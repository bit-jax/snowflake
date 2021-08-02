import snowflake.connector
from getpass import getpass
import secrets
import pandas as pd

conn = snowflake.connector.connect(
    user=secrets.username(),
    password=secrets.pw(),
    account=secrets.id(),
)

conn.cursor().execute("CREATE WAREHOUSE IF NOT EXISTS tiny_warehouse_mg")
conn.cursor().execute("CREATE DATABASE IF NOT EXISTS testdb_mg")
conn.cursor().execute("USE DATABASE testdb_mg")
conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS testschema_mg")

conn.cursor().execute("USE WAREHOUSE tiny_warehouse_mg")
conn.cursor().execute("USE DATABASE testdb_mg")
conn.cursor().execute("USE SCHEMA testdb_mg.testschema_mg")

conn.cursor().execute(
    "CREATE OR REPLACE TABLE "
    "test_stats(Rank integer, Position string, Name string, Age integer, G integer, PA integer, AB integer, R integer, H integer, secondB integer, thirdB integer, HR integer, RBI integer, SB integer, CS integer, BB integer, SO integer, BA decimal, OBP decimal, SLG decimal, OPS decimal, OPSplus integer, TB integer, GDP integer, HBP integer, SH integer, SF integer, IBB integer)"
)

data = pd.read_csv(r'./test_stats.txt')
df = pd.DataFrame(data)
filled_df = df.fillna(0)

for row in filled_df.itertuples():
    conn.cursor().execute('''
        INSERT INTO test_stats(Rank,Position,Name,Age,G,PA,AB,R,H,secondB,thirdB,HR,RBI,SB,CS,BB,SO,BA,OBP,SLG,OPS,OPSplus,TB,GDP,HBP,SH,SF,IBB)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ''',
        (row.Rank,
        row.Position,
        row.Name,
        row.Age,
        row.G,
        row.PA,
        row.AB,
        row.R,
        row.H,
        row.secondB,
        row.thirdB,
        row.HR,
        row.RBI,
        row.SB,
        row.CS,
        row.BB,
        row.SO,
        row.BA,
        row.OBP,
        row.SLG,
        row.OPS,
        row.OPSplus,
        row.TB,
        row.GDP,
        row.HBP,
        row.SH,
        row.SF,
        row.IBB)
    )