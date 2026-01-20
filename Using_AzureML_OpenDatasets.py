import IPython.display as display
from azureml.opendatasets import NoaaIsdWeather
from dateutil import parser


start_date = parser.parse("2019-01-01")
end_date = parser.parse("2019-03-31")
isd = NoaaIsdWeather(start_date, end_date)

# Create a dataframe from the dataset
df = isd.to_spark_dataframe()
display(df.limit(10))