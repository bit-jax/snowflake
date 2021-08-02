import snowflake.connector

# user = input("username ")
# pw = input('password ')
# Gets the version
ctx = snowflake.connector.connect(
    user='bitjax',
    password='Pbandj11!',
    account='ef17224.west-us-2.azure'
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()