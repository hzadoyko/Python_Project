import pandas as pd
import matplotlib.pyplot as plt

# Create dummy DataFrame
data = {'name': ['John', 'Mary', 'Sam', 'Sarah'], 
        'height': [183, 167, 174, 160],
        'weight': [89, 56, 68, 50]}
df = pd.DataFrame(data)

# Scatter plot height vs weight
df.plot.scatter(x='height', y='weight')

# Set labels and title
plt.xlabel('Height (cm)') 
plt.ylabel('Weight (kg)')
plt.title('Height vs Weight')

plt.tight_layout()
plt.show()

# Function to filter by height range 
def filter_height(df, min_ht, max_ht):
    return df[(df['height'] >= min_ht) & (df['height'] <= max_ht)]

# Filter rows between 160cm and 170cm height
result = filter_height(df, 160, 170)
print(result)