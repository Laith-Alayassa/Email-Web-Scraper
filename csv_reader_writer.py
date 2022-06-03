from pickle import TRUE
from pprint import  pprint
import pandas as pd



emails = {'test@gmail.com', 'test2@gmail.com'}
df = pd.DataFrame(columns=['Candidate Name', "emails"])
def read_data(file_name, *use_cols):
    data = pd.read_csv(f'{file_name}', usecols= list(use_cols))

    # for now, use a smaller set of data
    data_small = data.head(10)
    return data_small



data = read_data('jocjoc.csv', 'Candidate Name', 'State')

for i, name in enumerate(data["Candidate Name"]):
    df.loc[i, ["Candidate Name"]] = name
    df.at[i, ["emails"]] = str(list(emails))

df.to_csv('output_files testing.csv')
print(df)

