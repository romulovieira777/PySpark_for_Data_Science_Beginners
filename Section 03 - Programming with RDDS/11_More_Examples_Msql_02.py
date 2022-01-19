import numpy as np
import pandas as pd


num_cols = ["Accunt Balance", "No of Dependents"]
s = pd.Series([1, 2, 3])
s.describe()

s = pd.Series(["a", "a", "b", "c"])
s.describe()

s = pd.Series([np.datetime64("2000-01-01"), np.datetime64("2010-01-01"), np.datetime64("2010-01-01")])
s.describe()

df = pd.DataFrame({"categorical": pd.Categorical(["d", "e", "f"]), "numeric": [1, 2, 3], "object": ["a", "b", "c"]})
df.describe()
df.describe(include="all")
df.numeric.describe()
