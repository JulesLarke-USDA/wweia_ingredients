import pandas as pd

# Load data for FNDDS15-16
fndds2016 = pd.read_csv('../data/01/fndds_16_consolidated_ingredient_codes_050523.csv')
fndds2016.rename(columns={'Food code':'foodcode'}, inplace=True)

fndds16_ingred_1 = fndds2016[fndds2016['Ingredient code']>10000000].copy()
fndds16_ingred_1.rename(columns={'foodcode':'parent_foodcode', 'Main food description': 'parent_desc', 'Ingredient code': 'foodcode', 'Ingredient description': 'ingred_desc1', 'Ingredient weight': 'ingred_wt1'}, inplace=True)
fndds16_ingred_1 = fndds16_ingred_1[['parent_foodcode', 'parent_desc', 'foodcode', 'ingred_desc1', 'ingred_wt1']]

fndds16_ingred_2 = pd.merge(fndds16_ingred_1, fndds2016, how='left', on='foodcode')
fndds16_ingred_2.rename(columns={'foodcode':'ingred_code1', 'Ingredient code': 'foodcode', 'Ingredient description': 'ingred_desc2', 'Ingredient weight': 'ingred_wt2'}, inplace=True)
fndds16_ingred_2 = fndds16_ingred_2[['parent_foodcode', 'parent_desc', 'foodcode', 'ingred_code1', 'ingred_desc1', 'ingred_wt1', 'ingred_desc2', 'ingred_wt2']]

fndds16_ingred_3 = pd.merge(fndds16_ingred_2, fndds2016, how='left', on='foodcode')
fndds16_ingred_3.rename(columns={'foodcode':'ingred_code2', 'Ingredient code': 'foodcode', 'Ingredient description': 'ingred_desc3', 'Ingredient weight': 'ingred_wt3'}, inplace=True)
fndds16_ingred_3 = fndds16_ingred_3[['parent_foodcode', 'parent_desc', 'foodcode', 'ingred_code1', 'ingred_desc1', 'ingred_wt1', 'ingred_code2', 'ingred_desc2', 'ingred_wt2', 'ingred_desc3', 'ingred_wt3']]

fndds16_ingred_4 = pd.merge(fndds16_ingred_3, fndds2016, how='left', on='foodcode')
fndds16_ingred_4.rename(columns={'foodcode':'ingred_code3', 'Ingredient code': 'ingred_code4', 'Ingredient description': 'ingred_desc4', 'Ingredient weight': 'ingred_wt4'}, inplace=True)
fndds16_ingred_4 = fndds16_ingred_4[['parent_foodcode', 'parent_desc', 'ingred_code1', 'ingred_desc1', 'ingred_wt1', 'ingred_code2', 'ingred_desc2', 'ingred_wt2', 'ingred_code3', 'ingred_desc3', 'ingred_wt3', 'ingred_code4', 'ingred_desc4', 'ingred_wt4']]

# Load data for FNDDS17-18

fndds2018 = pd.read_csv('../data/01/fndds_18_consolidated_ingredient_codes_050523.csv')
fndds2018.rename(columns={'Food code':'foodcode', 'Ingredient weight (g)': 'Ingredient weight'}, inplace=True)

fndds18_ingred_1 = fndds2018[fndds2018['Ingredient code']>10000000].copy()
fndds18_ingred_1.rename(columns={'foodcode':'parent_foodcode', 'Main food description': 'parent_desc', 'Ingredient code': 'foodcode', 'Ingredient description': 'ingred_desc1', 'Ingredient weight': 'ingred_wt1'}, inplace=True)
fndds18_ingred_1 = fndds18_ingred_1[['parent_foodcode', 'parent_desc', 'foodcode', 'ingred_desc1', 'ingred_wt1']]

fndds18_ingred_2 = pd.merge(fndds18_ingred_1, fndds2018, how='left', on='foodcode')
fndds18_ingred_2.rename(columns={'foodcode':'ingred_code1', 'Ingredient code': 'foodcode', 'Ingredient description': 'ingred_desc2', 'Ingredient weight': 'ingred_wt2'}, inplace=True)
fndds18_ingred_2 = fndds18_ingred_2[['parent_foodcode', 'parent_desc', 'foodcode', 'ingred_code1', 'ingred_desc1', 'ingred_wt1', 'ingred_desc2', 'ingred_wt2']]

fndds18_ingred_3 = pd.merge(fndds18_ingred_2, fndds2018, how='left', on='foodcode')
fndds18_ingred_3.rename(columns={'foodcode':'ingred_code2', 'Ingredient code': 'foodcode', 'Ingredient description': 'ingred_desc3', 'Ingredient weight': 'ingred_wt3'}, inplace=True)
fndds18_ingred_3 = fndds18_ingred_3[['parent_foodcode', 'parent_desc', 'foodcode', 'ingred_code1', 'ingred_desc1', 'ingred_wt1', 'ingred_code2', 'ingred_desc2', 'ingred_wt2', 'ingred_desc3', 'ingred_wt3']]

fndds18_ingred_4 = pd.merge(fndds18_ingred_3, fndds2018, how='left', on='foodcode')
fndds18_ingred_4.rename(columns={'foodcode':'ingred_code3', 'Ingredient code': 'ingred_code4', 'Ingredient description': 'ingred_desc4', 'Ingredient weight': 'ingred_wt4'}, inplace=True)
fndds18_ingred_4 = fndds18_ingred_4[['parent_foodcode', 'parent_desc', 'ingred_code1', 'ingred_desc1', 'ingred_wt1', 'ingred_code2', 'ingred_desc2', 'ingred_wt2', 'ingred_code3', 'ingred_desc3', 'ingred_wt3', 'ingred_code4', 'ingred_desc4', 'ingred_wt4']]

fndds16_ingred_4.to_csv('../data/02/fndds_16_wts_to_correct_050523.csv', index=None)
fndds18_ingred_4.to_csv('../data/02/fndds_18_wts_to_correct_050523.csv', index=None)
