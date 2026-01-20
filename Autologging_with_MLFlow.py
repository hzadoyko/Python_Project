import mlflow
import mlflow.sklearn
import pandas as pd
import matplotlib.pyplot as plt

from numpy import savetxt
from sklearn.model_selection import train_test_split
from sklearn.datasets import load_diabetes
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

db = load_diabetes()
X = db.data
y = db.target
X_train, X_test, y_train, y_test = train_test_split(X, y)

# Enable autolog()
mlflow.sklearn.autolog()

# Once enabled, all model params, a model score, and the fitted model are logged automatically
with mlflow.start_run():

    # Set model parameters
    n_estimators = 100
    max_depth = 6
    max_features = 3

    # Create and train model
    rf = RandomForestRegressor(n_estimators=n_estimators,
                               max_depth=max_depth,
                               max_features=max_features)
    rf.fit(X_train, y_train)

    # Use the model to make predictions on the test data set
    predictions = rf.predict(X_test)

mlflow.end_run()