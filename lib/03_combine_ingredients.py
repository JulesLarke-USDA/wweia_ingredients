# Import packages
from calendar import c
import pandas as pd
import requests

# Load data for FNDDS ingredient values
fndds_16_url = 'https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2015-2016%20FNDDS%20At%20A%20Glance%20-%20Ingredient%20Nutrient%20Values.xlsx'
fndds_18_url = 'https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2017-2018%20FNDDS%20At%20A%20Glance%20-%20Ingredient%20Nutrient%20Values.xlsx'

fndds_16_url_content = requests.get(fndds_16_url).content
fndds_18_url_content = requests.get(fndds_18_url).content

fndds_16_xl = pd.ExcelFile(fndds_16_url_content)
fndds_18_xl = pd.ExcelFile(fndds_18_url_content)

nutrient_values_16 = fndds_16_xl.parse('Ingredient Nutrient Values', header = 1)
nutrient_values_18 = fndds_18_xl.parse('Ingredient Nutrient Values', header = 1)

nutrient_values_16.rename(columns={'SR description': 'Ingredient description'}, inplace=True)
nutrient_values_18.rename(columns={'SR description': 'Ingredient description'}, inplace=True)

milk_nfs = nutrient_values_16.loc[nutrient_values_16['Ingredient code'].isin([1077, 1079, 1082, 1085])].copy()

milk_nfs['average_nutrients'] = milk_nfs.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
milk_nfs = milk_nfs.drop_duplicates(subset='Nutrient description')
milk_nfs.drop(columns={'Nutrient value'}, inplace=True)
milk_nfs.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
milk_nfs['Ingredient code'] = 1111
milk_nfs['Ingredient description'] = 'Milk, averaged fat, with added vitamin A and D'

oil_table_fat_nfs = nutrient_values_16.loc[nutrient_values_16['Ingredient code'].isin([4613, 1001, 4694, 4044, 4518, 4582, 4053])].copy()

oil_table_fat_nfs['average_nutrients'] = oil_table_fat_nfs.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
oil_table_fat_nfs = oil_table_fat_nfs.drop_duplicates(subset='Nutrient description')
oil_table_fat_nfs.drop(columns={'Nutrient value'}, inplace=True)
oil_table_fat_nfs.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
oil_table_fat_nfs['Ingredient code'] = 4321
oil_table_fat_nfs['Ingredient description'] = 'Oil, table fat, averaged'

vegetable_oil_nfs = nutrient_values_16.loc[nutrient_values_16['Ingredient code'].isin([4044, 4518, 4582, 4053])].copy()

vegetable_oil_nfs['average_nutrients'] = vegetable_oil_nfs.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
vegetable_oil_nfs = vegetable_oil_nfs.drop_duplicates(subset='Nutrient description')
vegetable_oil_nfs.drop(columns={'Nutrient value'}, inplace=True)
vegetable_oil_nfs.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
vegetable_oil_nfs['Ingredient code'] = 4322
vegetable_oil_nfs['Ingredient description'] = 'Vegetable oil, averaged'

table_fat_nfs = nutrient_values_16.loc[nutrient_values_16['Ingredient code'].isin([1001, 4613, 4694])].copy()

table_fat_nfs['average_nutrients'] = table_fat_nfs.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
table_fat_nfs = table_fat_nfs.drop_duplicates(subset='Nutrient description')
table_fat_nfs.drop(columns={'Nutrient value'}, inplace=True)
table_fat_nfs.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
table_fat_nfs['Ingredient code'] = 4323
table_fat_nfs['Ingredient description'] = 'Table fat, averaged'

ground_beef_raw = nutrient_values_16.loc[nutrient_values_16['Ingredient code'].isin([23567, 23572, 23577, 23562, 23557])].copy()

ground_beef_raw['average_nutrients'] = ground_beef_raw.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
ground_beef_raw = ground_beef_raw.drop_duplicates(subset='Nutrient description')
ground_beef_raw.drop(columns={'Nutrient value'}, inplace=True)
ground_beef_raw.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
ground_beef_raw['Ingredient code'] = 23222
ground_beef_raw['Ingredient description'] = 'Ground beef, raw, averaged'

ground_beef_cooked = nutrient_values_16.loc[nutrient_values_16['Ingredient code'].isin([23578, 23573, 23568, 23563, 2047])].copy()

ground_beef_cooked['average_nutrients'] = ground_beef_cooked.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
ground_beef_cooked = ground_beef_cooked.drop_duplicates(subset='Nutrient description')
ground_beef_cooked.drop(columns={'Nutrient value'}, inplace=True)
ground_beef_cooked.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
ground_beef_cooked['Ingredient code'] = 23223
ground_beef_cooked['Ingredient description'] = 'Ground beef, cooked, averaged'

nutrient_values_16 = pd.concat([nutrient_values_16, milk_nfs, oil_table_fat_nfs, vegetable_oil_nfs, table_fat_nfs, ground_beef_raw, ground_beef_cooked])

nutrients_16 = pd.pivot(nutrient_values_16, index=['Ingredient code', 'Ingredient description'], columns='Nutrient description', values='Nutrient value')

nutrients_16.reset_index(inplace=True)

nutrients_16.rename(columns={'4:0': 'Butyric acid', '6:0': 'Caproic acid', '8:0': 'Caprylic acid', '10:0': 'Capric acid', '12:0': 'Lauric acid', '14:0': 'Myristic acid', '16:0': 'Palmitic acid', '16:1': 'Palmitoleic acid', '18:0': 'Stearic acid', '18:1': 'Oleic acid', '18:2': 'Linoleic acid', '18:3': 'Linolenic acid', '18:4': 'Stearidonic acid', '20:1': 'Eicosenoic acid', '20:4': 'Arachidonic acid', '20:5 n-3': 'Eicosapentaenoic acid', '22:1': 'Erucic acid', '22:5 n-3': 'Docosapentaenoic acid', '22:6 n-3': 'Docosahexaenoic acid'}, inplace=True)

milk_nfs = nutrient_values_18.loc[nutrient_values_18['Ingredient code'].isin([1077, 1079, 1082, 1085])].copy()

milk_nfs['average_nutrients'] = milk_nfs.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
milk_nfs = milk_nfs.drop_duplicates(subset='Nutrient description')
milk_nfs.drop(columns={'Nutrient value'}, inplace=True)
milk_nfs.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
milk_nfs['Ingredient code'] = 1111
milk_nfs['Ingredient description'] = 'Milk, averaged fat, with added vitamin A and D'

oil_table_fat_nfs = nutrient_values_18.loc[nutrient_values_18['Ingredient code'].isin([4613, 1001, 4694, 4044, 4518, 4582, 4053])].copy()

oil_table_fat_nfs['average_nutrients'] = oil_table_fat_nfs.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
oil_table_fat_nfs = oil_table_fat_nfs.drop_duplicates(subset='Nutrient description')
oil_table_fat_nfs.drop(columns={'Nutrient value'}, inplace=True)
oil_table_fat_nfs.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
oil_table_fat_nfs['Ingredient code'] = 4321
oil_table_fat_nfs['Ingredient description'] = 'Oil, table fat, averaged'

table_fat_nfs = nutrient_values_18.loc[nutrient_values_18['Ingredient code'].isin([1001, 4613, 4694])].copy()

table_fat_nfs['average_nutrients'] = table_fat_nfs.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
table_fat_nfs = table_fat_nfs.drop_duplicates(subset='Nutrient description')
table_fat_nfs.drop(columns={'Nutrient value'}, inplace=True)
table_fat_nfs.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
table_fat_nfs['Ingredient code'] = 4323
table_fat_nfs['Ingredient description'] = 'Table fat, averaged'

ground_beef_raw = nutrient_values_18.loc[nutrient_values_18['Ingredient code'].isin([23567, 23572, 23577, 23562, 23557])].copy()

ground_beef_raw['average_nutrients'] = ground_beef_raw.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
ground_beef_raw = ground_beef_raw.drop_duplicates(subset='Nutrient description')
ground_beef_raw.drop(columns={'Nutrient value'}, inplace=True)
ground_beef_raw.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
ground_beef_raw['Ingredient code'] = 23222
ground_beef_raw['Ingredient description'] = 'Ground beef, raw, averaged'

ground_beef_cooked = nutrient_values_18.loc[nutrient_values_18['Ingredient code'].isin([23578, 23573, 23568, 23563, 2047])].copy()

ground_beef_cooked['average_nutrients'] = ground_beef_cooked.groupby(['Nutrient description'])['Nutrient value'].transform('mean')
ground_beef_cooked = ground_beef_cooked.drop_duplicates(subset='Nutrient description')
ground_beef_cooked.drop(columns={'Nutrient value'}, inplace=True)
ground_beef_cooked.rename(columns={'average_nutrients':'Nutrient value'}, inplace=True)
ground_beef_cooked['Ingredient code'] = 23223
ground_beef_cooked['Ingredient description'] = 'Ground beef, cooked, averaged'

nutrient_values_18 = pd.concat([nutrient_values_18, milk_nfs, oil_table_fat_nfs, vegetable_oil_nfs, table_fat_nfs, ground_beef_raw, ground_beef_cooked])

nutrients_18 = pd.pivot(nutrient_values_18, index=['Ingredient code', 'Ingredient description'], columns='Nutrient description', values='Nutrient value')

nutrients_18.reset_index(inplace=True)

nutrients_18.rename(columns={'4:0': 'Butyric acid', '6:0': 'Caproic acid', '8:0': 'Caprylic acid', '10:0': 'Capric acid', '12:0': 'Lauric acid', '14:0': 'Myristic acid', '16:0': 'Palmitic acid', '16:1': 'Palmitoleic acid', '18:0': 'Stearic acid', '18:1': 'Oleic acid', '18:2': 'Linoleic acid', '18:3': 'Linolenic acid', '18:4': 'Stearidonic acid', '20:1': 'Eicosenoic acid', '20:4': 'Arachidonic acid', '20:5 n-3': 'Eicosapentaenoic acid', '22:1': 'Erucic acid', '22:5 n-3': 'Docosapentaenoic acid', '22:6 n-3': 'Docosahexaenoic acid'}, inplace=True)

pd.concat([nutrients_16, nutrients_18], axis=0).drop_duplicates(subset='Ingredient code').to_csv('../data/03/fndds_15_18_all_ingredient_nutrient_vals_050523.csv', index=None)