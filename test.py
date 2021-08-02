import pandas as pd

data = pd.read_csv(r'./test_stats.txt')
df = pd.DataFrame(data)

for row in df.itertuples():
    cursor.execute(
        
    )
