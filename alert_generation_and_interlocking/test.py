import pandas as pd
import pprint

yes = [
    [1,4,7],
    [2,5,8],
    [3,6,9]
    ]
df = pd.DataFrame(yes)

df1 = df.describe(percentiles=[0.95]).to_dict()
# means = [i for i in df.mean()]
# pprint.pprint(means)
# quartile_95 = [i for i in df.quantile(0.95)]
# pprint.pprint(quartile_95)
print(df1)
threshold_dict = df1
for i,j in df1.items():
    print(j)
    for k in j.keys():
        if k not in ['mean','95%']:
            del threshold_dict[i][k]


print(threshold_dict)