from utility.file_manager import FileManager

from constants import RAW_DATA_PATH, CLEAN_DATA_PATH, DOCUMENTATION_PATH

# Initialize FileManager objects for clean data and documentation
clean_manager = FileManager(CLEAN_DATA_PATH)

steps = clean_manager.load_file('steps.xlsx')


# Import required modules
import pandas as pd  # For data manipulation
from plotly.offline import plot
import plotly.graph_objs as go  # For plotting objects

# Assuming that 'steps' contains the DataFrame from your code
# If not, uncomment the line below
# steps = pd.read_excel('path/to/your/steps.xlsx')

# Convert 'Date' column to datetime type
steps['date'] = pd.to_datetime(steps['date'])

# Set 'Date' as the index
steps.set_index('date', inplace=True)

# Aggregate the steps by date
steps_daily = steps.resample('D').sum().reset_index()

# Create the interactive plot
trace = go.Scatter(x=steps_daily['date'], y=steps_daily['stepcount'], mode='lines+markers')
layout = go.Layout(title='Total Steps per Day Over Multiple Years')
fig = go.Figure(data=[trace], layout=layout)

# Save the plot as an HTML file
plot(fig, filename='steps_plot.html', auto_open=False)
