import numpy as np
import pandas as pd
import pyspark.pandas as ps

# Create a pandas Series
pser = pd.Series([1, 3, 5, np.nan, 6, 8])

# Create a pandas-on-Spark Series
psser = ps.Series([1, 3, 5, np.nan, 6, 8])

# Create a pandas-on-Spark Series by passing a pandas Series
psser = ps.Series(pser)
psser = ps.from_pandas(pser)

# Show output of each Series
pser

psser

psser.sort_index()

# Create a pandas DataFrame
pdf = pd.DataFrame({'A': np.random.rand(5),
                    'B' : np.random.rand(5)})

# Create a pandas-on-Spark DataFrame
psdf = ps.DataFrame({'A': np.random.rand(5),
                     'B' : np.random.rand(5)})

# Create a pandas-on-Spark DataFrame by passing a pandas DataFrame
psdf = ps.DataFrame(pdf)
psdf = ps.from_pandas(pdf)

# Show output of each DataFrame
pdf

psdf
psdf.sort_index()

psdf.head(2)

psdf.describe()

# Sort pandas-on-Spark DataFrame by column 'B'
psdf.sort_values(by= 'B')

# Transpose pandas-on-Spark DataFrame to make columns as rows
psdf.transpose()

ps.get_option('compute.max_rows')

# Change the maximum number of rows to display
ps.set_option('compute.max_rows', 2000)
ps.get_option('compute.max_rows')

# Show pandas-on-Spark column A
psdf['A']

psdf[['A', 'B']]

psdf.loc[1:2]

psdf.iloc[:3, 1:2]

psser = ps.Series([100, 200, 300, 400, 500], index = [0, 1, 2, 3, 4])

# Set option to allow operations on different frames
from pyspark.pandas.config import set_option, reset_option
set_option("compute.ops_on_diff_frames" , True)
psdf['C'] = psser

# Set the option back to default value to avoid expensive operation in the future
reset_option("compute.ops_on_diff_frames")

psdf

# Apply cumulative sum function on pandas-on-Spark DataFrame
psdf.apply(np.cumsum)

psdf.apply(np.cumsum, axis = 1)

# Apply lambda function to square each element in pandas-on-Spark DataFrame
psdf.apply(lambda x: x ** 2)

# Create a function to apply to pandas-on-Spark DataFrame
def square(x) -> ps.Series[np.float64]:
    return x ** 2

psdf.apply(square)

# Create a large pandas-on-Spark DataFrame and apply a function
# First one will work, second one will raise an error due to size of data > compute.shortcut_limit (1000)
ps.DataFrame({'A': range(1000)}).apply(lambda col: col.max())

ps.DataFrame({'A': range(1001)}).apply(lambda col: col.max())