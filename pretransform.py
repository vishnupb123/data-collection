import pandas as pd
import numpy as np

df = pd.read_csv('twitter_data.csv' , encoding='utf-8' , low_memory=False)

df.describe()
df.shape