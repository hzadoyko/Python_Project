import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Import Life Expectancy data into data frame
life_df = pd.read_csv("expectancy.csv")

# Rename existing column and copy that info into new column with original name
life_df.rename(columns = {"mf-life-expectancy": "both"}, inplace=True)
life_df["mf-life-expectancy"] = life_df["both"]

# Check out firs 75 rows of data in data frame
life_df.head(75)

# Import happiness data into another data frame and view top 5 rows
happiness_df = pd.read_csv("happiness.csv")
happiness_df.head()

# Create new data frame with merged results from both existing data frames
# Will be joined on the common column "country"
df = happiness_df.merge(life_df)
df.head(25)

# Create data frame with only numeric values to find correlations
numeric_df = df.select_dtypes(include='number')
numeric_df.corr()

# Create new data frame with only the top 50 countries
df_fifty = df.head(50)

# Create a scatterplot with these values
g = sns.regplot(x = "mf-life-expectancy", y = "happiness2021", data = df_fifty, scatter_kws = {'color': 'purple'}, line_kws = {'color': 'purple', 'linestyle': '--'})
g.figure.set_size_inches(10,10)
g.figure.text(0.45, 0.55, 'United States', fontsize = 14)
g.figure.suptitle("Top 50 Countries Life Expectancy and Happiness 2021")