import snowflake.connector
import secrets

# user = input("username ")
# pw = input('password ')
# Gets the version
ctx = snowflake.connector.connect(
    user=secrets.username(),
    password=secrets.pw(),
    account=secrets.id()
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()