from datasets import load_dataset
import pandas as pd

dataset = load_dataset("hate_speech18")

# Convert the 'train' part of the dataset to a DataFrame
df = pd.DataFrame(dataset['train'])

# Filter the DataFrame to only include rows where 'label' is 1
hate_speech = df[df['label'] == 1]

# print in terminal to see resulting dataframe
print(hate_speech.head())

# Export the dataframe as csv
hate_speech.to_csv('hf_hate_speech.csv', index=False)