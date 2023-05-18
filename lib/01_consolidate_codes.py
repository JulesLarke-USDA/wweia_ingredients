# Import packages
import pandas as pd
import requests

# Load data
fndds_16_url = 'https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2015-2016%20FNDDS%20At%20A%20Glance%20-%20FNDDS%20Ingredients.xlsx'
fndds_18_url = 'https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2017-2018%20FNDDS%20At%20A%20Glance%20-%20FNDDS%20Ingredients.xlsx'

fndds_16_url_content = requests.get(fndds_16_url).content
fndds_18_url_content = requests.get(fndds_18_url).content

fndds_16_xl = pd.ExcelFile(fndds_16_url_content)
fndds_18_xl = pd.ExcelFile(fndds_18_url_content)

fndds_16 = fndds_16_xl.parse('FNDDS Ingredients', header = 1)
fndds_18 = fndds_18_xl.parse('FNDDS Ingredients', header = 1)

# Replace codes
fndds_16.replace({'Ingredient code': 11100000}, 1111, inplace=True)
fndds_16.replace({'Ingredient description': 'Milk, NFS'}, 'Milk, averaged fat, with added vitamin A and D', inplace=True)

fndds_16.replace({'Ingredient code': 81200100}, 4321, inplace=True)
fndds_16.replace({'Ingredient description': 'Oil or table fat, NFS'}, 'Oil, table fat, averaged', inplace=True)

fndds_16.replace({'Ingredient code': 21500000}, 23222, inplace=True)
fndds_16.replace({'Ingredient description': 'Ground beef, raw'}, 'Ground beef, raw, averaged', inplace=True)

fndds_16.replace({'Ingredient code': 21500100}, 23223, inplace=True)
fndds_16.replace({'Ingredient description': 'Ground beef, cooked'}, 'Ground beef, cooked, averaged', inplace=True)

fndds_16.replace({'Ingredient code': 82101000}, 4322, inplace=True)
fndds_16.replace({'Ingredient description': 'Vegetable oil, NFS'}, 'Vegetable oil, averaged', inplace=True)

fndds_16.replace({'Ingredient code': 81100000}, 4323, inplace=True)
fndds_16.replace({'Ingredient description': 'Table fat, NFS'}, 'Table fat, averaged', inplace=True)

fndds_16.loc[fndds_16['Food code'].isin([11100000])] = fndds_16.loc[fndds_16['Food code'].isin([11100000])].drop_duplicates(subset='Food code')
fndds_16.loc[fndds_16['Food code'].isin([81200100])] = fndds_16.loc[fndds_16['Food code'].isin([81200100])].drop_duplicates(subset='Food code')
fndds_16.loc[fndds_16['Food code'].isin([21500000])] = fndds_16.loc[fndds_16['Food code'].isin([21500000])].drop_duplicates(subset='Food code')
fndds_16.loc[fndds_16['Food code'].isin([21500100])] = fndds_16.loc[fndds_16['Food code'].isin([21500100])].drop_duplicates(subset='Food code')
fndds_16.loc[fndds_16['Food code'].isin([82101000])] = fndds_16.loc[fndds_16['Food code'].isin([82101000])].drop_duplicates(subset='Food code')
fndds_16.loc[fndds_16['Food code'].isin([81100000])] = fndds_16.loc[fndds_16['Food code'].isin([81100000])].drop_duplicates(subset='Food code')

fndds_16 = fndds_16.dropna()

fndds_16.loc[fndds_16['Food code'] == 11100000, 'Ingredient code'] = 1111
fndds_16.loc[fndds_16['Food code'] == 11100000, 'Ingredient description'] = 'Milk, averaged fat, with added vitamin A and D'

fndds_16.loc[fndds_16['Food code'] == 81200100, 'Ingredient code'] = 4321
fndds_16.loc[fndds_16['Food code'] == 81200100, 'Ingredient description'] = 'Oil, table fat, averaged'

fndds_16.loc[fndds_16['Food code'] == 21500000, 'Ingredient code'] = 23222
fndds_16.loc[fndds_16['Food code'] == 21500000, 'Ingredient description'] = 'Ground beef, raw, averaged'

fndds_16.loc[fndds_16['Food code'] == 21500100, 'Ingredient code'] = 23223
fndds_16.loc[fndds_16['Food code'] == 21500100, 'Ingredient description'] = 'Ground beef, cooked, averaged'

fndds_16.loc[fndds_16['Food code'] == 82101000, 'Ingredient code'] = 4322
fndds_16.loc[fndds_16['Food code'] == 82101000, 'Ingredient description'] = 'Vegetable oil, averaged'

fndds_16.loc[fndds_16['Food code'] == 81100000, 'Ingredient code'] = 4323
fndds_16.loc[fndds_16['Food code'] == 81100000, 'Ingredient description'] = 'Table fat, averaged'

# FNDDS 17-18
fndds_18.replace({'Ingredient code': 11100000}, 1111, inplace=True)
fndds_18.replace({'Ingredient description': 'Milk, NFS'}, 'Milk, averaged fat, with added vitamin A and D', inplace=True)

fndds_18.replace({'Ingredient code': 81200100}, 4321, inplace=True)
fndds_18.replace({'Ingredient description': 'Oil or table fat, NFS'}, 'Oil, table fat, averaged', inplace=True)

fndds_18.replace({'Ingredient code': 21500000}, 23222, inplace=True)
fndds_18.replace({'Ingredient description': 'Ground beef, raw'}, 'Ground beef, raw, averaged', inplace=True)

fndds_18.replace({'Ingredient code': 21500100}, 23223, inplace=True)
fndds_18.replace({'Ingredient description': 'Ground beef, cooked'}, 'Ground beef, cooked, averaged', inplace=True)

fndds_18.replace({'Ingredient code': 82101000}, 4322, inplace=True)
fndds_18.replace({'Ingredient description': 'Vegetable oil, NFS'}, 'Vegetable oil, averaged', inplace=True)

fndds_18.replace({'Ingredient code': 81100000}, 4323, inplace=True)
fndds_18.replace({'Ingredient description': 'Table fat, NFS'}, 'Table fat, averaged', inplace=True)

fndds_18.loc[fndds_18['Food code'].isin([11100000])] = fndds_18.loc[fndds_18['Food code'].isin([11100000])].drop_duplicates(subset='Food code')
fndds_18.loc[fndds_18['Food code'].isin([81200100])] = fndds_18.loc[fndds_18['Food code'].isin([81200100])].drop_duplicates(subset='Food code')
fndds_18.loc[fndds_18['Food code'].isin([21500000])] = fndds_18.loc[fndds_18['Food code'].isin([21500000])].drop_duplicates(subset='Food code')
fndds_18.loc[fndds_18['Food code'].isin([21500100])] = fndds_18.loc[fndds_18['Food code'].isin([21500100])].drop_duplicates(subset='Food code')
fndds_18.loc[fndds_18['Food code'].isin([82101000])] = fndds_18.loc[fndds_18['Food code'].isin([82101000])].drop_duplicates(subset='Food code')
fndds_18.loc[fndds_18['Food code'].isin([81100000])] = fndds_18.loc[fndds_18['Food code'].isin([81100000])].drop_duplicates(subset='Food code')

fndds_18 = fndds_18.dropna()

fndds_18.loc[fndds_18['Food code'] == 11100000, 'Ingredient code'] = 1111
fndds_18.loc[fndds_18['Food code'] == 11100000, 'Ingredient description'] = 'Milk, averaged fat, with added vitamin A and D'

fndds_18.loc[fndds_18['Food code'] == 81200100, 'Ingredient code'] = 4321
fndds_18.loc[fndds_18['Food code'] == 81200100, 'Ingredient description'] = 'Oil, table fat, averaged'

fndds_18.loc[fndds_18['Food code'] == 21500000, 'Ingredient code'] = 23222
fndds_18.loc[fndds_18['Food code'] == 21500000, 'Ingredient description'] = 'Ground beef, raw, averaged'

fndds_18.loc[fndds_18['Food code'] == 21500100, 'Ingredient code'] = 23223
fndds_18.loc[fndds_18['Food code'] == 21500100, 'Ingredient description'] = 'Ground beef, cooked, averaged'

fndds_18.loc[fndds_18['Food code'] == 82101000, 'Ingredient code'] = 4322
fndds_18.loc[fndds_18['Food code'] == 82101000, 'Ingredient description'] = 'Vegetable oil, averaged'

fndds_18.loc[fndds_18['Food code'] == 81100000, 'Ingredient code'] = 4323
fndds_18.loc[fndds_18['Food code'] == 81100000, 'Ingredient description'] = 'Table fat, averaged'

fndds_16.to_csv('../data/01/fndds_16_consolidated_ingredient_codes_050523.csv', index=None)
fndds_18.to_csv('../data/01/fndds_18_consolidated_ingredient_codes_050523.csv', index=None)