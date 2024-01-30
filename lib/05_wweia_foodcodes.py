# Import packages
import pandas as pd
import dask.dataframe as dd
import numpy as np

print('Download & Read SAS Transport Files from web') 
# Demographic, Dietary day 1, and Food description data for cycles 01 through 18
demo_B = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2001-2002/demo_b.xpt', format='xport', encoding='utf-8')
iff_1_B = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2001-2002/drxiff_b.xpt', format='xport', encoding='utf-8')
food_desc_B = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2001-2002/drxfmt_b.xpt', format='xport', encoding='utf-8')
food_desc_B = food_desc_B.drop(columns='FMTNAME').rename(columns={'START': 'DRXFDCD', 'LABEL': 'DRXFCSD'})
food_desc_B['DRXFCLD'] = food_desc_B['DRXFCSD'] # Create duplicate column of short description labeled as long description for purpose of binding dataframes

demo_C = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2003-2004/demo_c.xpt', format='xport', encoding='utf-8')
iff_1_C = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2003-2004/dr1iff_c.xpt', format='xport', encoding='utf-8')
iff_2_C = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2003-2004/dr2iff_c.xpt', format='xport', encoding='utf-8')
food_desc_C = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2003-2004/drxfcd_c.xpt', format='xport', encoding='utf-8')

demo_D = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2005-2006/demo_d.xpt', format='xport', encoding='utf-8')
iff_1_D = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2005-2006/dr1iff_d.xpt', format='xport', encoding='utf-8')
iff_2_D = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2005-2006/dr2iff_d.xpt', format='xport', encoding='utf-8')
food_desc_D = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2005-2006/drxfcd_d.xpt', format='xport', encoding='utf-8')

demo_E = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2007-2008/DEMO_E.xpt', format='xport', encoding='utf-8')
iff_1_E = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2007-2008/DR1IFF_E.xpt', format='xport', encoding='utf-8')
iff_2_E = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2007-2008/DR2IFF_E.xpt', format='xport', encoding='utf-8')
food_desc_E = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2007-2008/drxfcd_e.xpt', format='xport', encoding='utf-8')

demo_F = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2009-2010/DEMO_f.xpt', format='xport', encoding='utf-8')
iff_1_F = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2009-2010/DR1IFF_f.xpt', format='xport', encoding='utf-8')
iff_2_F = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2009-2010/DR2IFF_f.xpt', format='xport', encoding='utf-8')
food_desc_F = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2009-2010/drxfcd_f.xpt', format='xport', encoding='utf-8')

demo_G = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2011-2012/DEMO_g.xpt', format='xport', encoding='utf-8')
iff_1_G = pd.read_sas('https://wwwn.cdc.gov/Nchs/Nhanes/2011-2012/DR1IFF_G.XPT', format='xport', encoding='utf-8')
iff_2_G = pd.read_sas('https://wwwn.cdc.gov/Nchs/Nhanes/2011-2012/DR2IFF_g.xpt', format='xport', encoding='utf-8')
food_desc_G = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2011-2012/drxfcd_g.xpt', format='xport', encoding='utf-8')

demo_H = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2013-2014/DEMO_h.xpt', format='xport', encoding='utf-8')
iff_1_H = pd.read_sas('https://wwwn.cdc.gov/nchs/nhanes/2013-2014/DR1IFF_H.XPT', format='xport', encoding='utf-8')
iff_2_H = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2013-2014/DR2IFF_h.xpt', format='xport', encoding='utf-8')
food_desc_H = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2013-2014/drxfcd_h.xpt', format='xport', encoding='latin-1') # error: 'utf-8' codec can't decode. using 'latin-1' works

demo_I = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2015-2016/DEMO_I.xpt', format='xport', encoding='utf-8')
iff_1_I = pd.read_sas('https://wwwn.cdc.gov/nchs/nhanes/2015-2016/DR1IFF_I.XPT', format='xport', encoding='utf-8')
iff_2_I = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2015-2016/DR2IFF_I.xpt', format='xport', encoding='utf-8')
food_desc_I = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2015-2016/drxfcd_i.xpt', format='xport', encoding='utf-8')

demo_J = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2017-2018/DEMO_J.xpt', format='xport', encoding='utf-8')
iff_1_J = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2017-2018/DR1IFF_J.xpt', format='xport', encoding='utf-8')
iff_2_J = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2017-2018/DR2IFF_J.xpt', format='xport', encoding='utf-8')
food_desc_J = pd.read_sas('https://wwwn.cdc.gov/NCHS/nhanes/2017-2018/drxfcd_j.xpt', format='xport', encoding='utf-8')

print('Finished downloading NHANES files; wrangling data') 
# select variables of interest from demographic and dietary data
demo_B = demo_B[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']]
iff_1_B = iff_1_B[['SEQN', 'DRDDRSTZ', 'DRXILINE', 'DRDIFDCD', 'DRD030Z', 'WTDRD1', 'DRXIGRMS', 'DRXIMOIS', 'DRXIKCAL', 'DRXICARB', 'DRXISUGR', 'DRXIFIBE']].rename(columns={'DRXILINE':'DR2ILINE','DRDIFDCD':'DRXFDCD','DRD030Z':'DR2_030Z','DRXIGRMS':'DR2IGRMS', 'DRXIMOIS': 'DR2IMOIS', 'DRXIKCAL':'DR2IKCAL','DRXICARB':'DR2ICARB','DRXISUGR':'DR2ISUGR','DRXIFIBE':'DR2IFIBE', 'DRDDRSTZ':'DR2DRSTZ'})
iff_1_B['diet_day'] = 1
food_desc_B.drop(columns='DRXFCSD', inplace=True)
food_desc_B.loc[-1] = [94000000, 'WATER AS AN INGREDIENT'] # add this food code and description since it was not included for some reason
recall_1_B = pd.merge(food_desc_B, iff_1_B, on = 'DRXFDCD')
recall_1_B_ = pd.merge(recall_1_B, demo_B, on = 'SEQN')
recall_1_B_.rename(columns={'WTDRD1':'diet_wts'},inplace=True)
recall_1_B_['CYCLE'] = '01_02'
recall_1_B_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_B_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_B_['DMDBORN'].replace({1 : "US", 2 : "Mexico", 3 : "Elsewhere", 7 : 'Unknown'}, inplace=True)
recall_1_B_['DR2_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_C = demo_C[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']]
iff_1_C = iff_1_C[['SEQN', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IMOIS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IMOIS':'DR2IMOIS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_1_C['diet_day'] = 1
iff_2_C = iff_2_C[['SEQN', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IMOIS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_C['DR2ILINE'] = iff_2_C['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_C['diet_day'] = 2
iff_C = pd.concat([iff_1_C, iff_2_C])
iff_C['diet_wts'] = np.where(iff_C['WTDR2D'].notna(), iff_C['WTDR2D'], iff_C['WTDRD1'])
iff_C = iff_C.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_C.drop(columns='DRXFCSD', inplace=True)
recall_1_C = pd.merge(food_desc_C, iff_C, on = 'DRXFDCD')
recall_1_C_ = pd.merge(recall_1_C, demo_C, on = 'SEQN')
recall_1_C_['CYCLE'] = '03_04'
recall_1_C_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_C_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_C_['DMDBORN'].replace({1 : "US", 2 : "Mexico", 3 : "Elsewhere", 7 : 'Unknown'}, inplace=True)
recall_1_C_['DR2_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  4 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  7 : 'Drink',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  18 : 'Snack',
  19 : 'Drink',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_D = demo_D[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']]
iff_1_D = iff_1_D[['SEQN', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IMOIS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IMOIS':'DR2IMOIS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_1_D['diet_day'] = 1
iff_2_D = iff_2_D[['SEQN', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IMOIS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_D['DR2ILINE'] = iff_2_D['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_D['diet_day'] = 2
iff_D = pd.concat([iff_1_D, iff_2_D])
iff_D['diet_wts'] = np.where(iff_D['WTDR2D'].notna(), iff_D['WTDR2D'], iff_D['WTDRD1'])
iff_D = iff_D.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_D.drop(columns='DRXFCSD', inplace=True)
recall_1_D = pd.merge(food_desc_D, iff_D, on = 'DRXFDCD')
recall_1_D_ = pd.merge(recall_1_D, demo_D, on = 'SEQN')
recall_1_D_['CYCLE'] = '05_06'
recall_1_D_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_D_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_D_['DMDBORN'].replace({1 : "US", 2 : "Mexico", 3 : "Elsewhere", 7 : 'Unknown'}, inplace=True)
recall_1_D_['DR2_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  4 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  7 : 'Drink',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  18 : 'Snack',
  19 : 'Drink',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_E = demo_E[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN2', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN2':'DMDBORN'})
iff_1_E = iff_1_E[['SEQN', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IMOIS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IMOIS':'DR2IMOIS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_1_E['diet_day'] = 1
iff_2_E = iff_2_E[['SEQN', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IMOIS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_E['DR2ILINE'] = iff_2_E['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_E['diet_day'] = 2
iff_E = pd.concat([iff_1_E, iff_2_E])
iff_E['diet_wts'] = np.where(iff_E['WTDR2D'].notna(), iff_E['WTDR2D'], iff_E['WTDRD1'])
iff_E = iff_E.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_E.drop(columns='DRXFCSD', inplace=True)
recall_1_E = pd.merge(food_desc_E, iff_E, on = 'DRXFDCD')
recall_1_E_ = pd.merge(recall_1_E, demo_E, on = 'SEQN')
recall_1_E_['CYCLE'] = '07_08'
recall_1_E_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_E_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_E_['DMDBORN'].replace({1 : "US", 2 : "Mexico", 4 : "Other_Spanish_Speaking_Country", 5 : 'Other_Non-Spanish_Speaking_Country', 7 : 'Unknown', 9 : 'Unknown'}, inplace=True)
recall_1_E_['DR2_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_F = demo_F[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN2', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN2':'DMDBORN'})
iff_1_F = iff_1_F[['SEQN', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IMOIS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IMOIS':'DR2IMOIS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_1_F['diet_day'] = 1
iff_2_F = iff_2_F[['SEQN', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IMOIS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_F['DR2ILINE'] = iff_2_F['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_F['diet_day'] = 2
iff_F = pd.concat([iff_1_F, iff_2_F])
iff_F['diet_wts'] = np.where(iff_F['WTDR2D'].notna(), iff_F['WTDR2D'], iff_F['WTDRD1'])
iff_F = iff_F.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_F.drop(columns='DRXFCSD', inplace=True)
recall_1_F = pd.merge(food_desc_F, iff_F, on = 'DRXFDCD')
recall_1_F_ = pd.merge(recall_1_F, demo_F, on = 'SEQN')
recall_1_F_['CYCLE'] = '09_10'
recall_1_F_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_F_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_F_['DMDBORN'].replace({1 : "US", 2 : "Mexico", 4 : "Other_Spanish_Speaking_Country", 5 : 'Other_Non-Spanish_Speaking_Country', 7 : 'Unknown', 9 : 'Unknown'}, inplace=True)
recall_1_F_['DR2_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  4 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  7 : 'Drink',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  18 : 'Snack',
  19 : 'Drink',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_G = demo_G[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN4', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN4':'DMDBORN'})
iff_1_G = iff_1_G[['SEQN', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IMOIS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IMOIS':'DR2IMOIS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_1_G['diet_day'] = 1
iff_2_G = iff_2_G[['SEQN', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IMOIS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_G['DR2ILINE'] = iff_2_G['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_G['diet_day'] = 2
iff_G = pd.concat([iff_1_G, iff_2_G])
iff_G['diet_wts'] = np.where(iff_G['WTDR2D'].notna(), iff_G['WTDR2D'], iff_G['WTDRD1'])
iff_G = iff_G.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_G.drop(columns='DRXFCSD', inplace=True)
recall_1_G = pd.merge(food_desc_G, iff_G, on = 'DRXFDCD')
recall_1_G_ = pd.merge(recall_1_G, demo_G, on = 'SEQN')
recall_1_G_['CYCLE'] = '11_12'
recall_1_G_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_G_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_G_['DMDBORN'].replace({1 : "US", 2 : "Elsewhere", 77 : 'Unknown', 99 : 'Unknown'}, inplace=True)
recall_1_G_['DR2_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  4 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  7 : 'Drink',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  18 : 'Snack',
  19 : 'Drink',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_H = demo_H[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN4', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN4':'DMDBORN'})
iff_1_H = iff_1_H[['SEQN', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IMOIS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IMOIS':'DR2IMOIS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_1_H['diet_day'] = 1
iff_2_H = iff_2_H[['SEQN', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IMOIS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_H['DR2ILINE'] = iff_2_H['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_H['diet_day'] = 2
iff_H = pd.concat([iff_1_H, iff_2_H])
iff_H['diet_wts'] = np.where(iff_H['WTDR2D'].notna(), iff_H['WTDR2D'], iff_H['WTDRD1'])
iff_H = iff_H.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_H.drop(columns='DRXFCSD', inplace=True)
recall_1_H = pd.merge(food_desc_H, iff_H, on = 'DRXFDCD')
recall_1_H_ = pd.merge(recall_1_H, demo_H, on = 'SEQN')
recall_1_H_['CYCLE'] = '13_14'
recall_1_H_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_H_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_H_['DMDBORN'].replace({1 : "US", 2 : "Elsewhere", 77 : 'Unknown', 99 : 'Unknown'}, inplace=True)
recall_1_H_['DR2_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  4 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  7 : 'Drink',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  18 : 'Snack',
  19 : 'Drink',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_I = demo_I[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN4', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN4':'DMDBORN'})
iff_1_I = iff_1_I[['SEQN', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IMOIS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IMOIS':'DR2IMOIS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_1_I['diet_day'] = 1
iff_2_I = iff_2_I[['SEQN', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IMOIS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_I['DR2ILINE'] = iff_2_I['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_I['diet_day'] = 2
iff_I = pd.concat([iff_1_I, iff_2_I])
iff_I['diet_wts'] = np.where(iff_I['WTDR2D'].notna(), iff_I['WTDR2D'], iff_I['WTDRD1'])
iff_I = iff_I.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_I.drop(columns='DRXFCSD', inplace=True)
recall_1_I = pd.merge(food_desc_I, iff_I, on = 'DRXFDCD')
recall_1_I_ = pd.merge(recall_1_I, demo_I, on = 'SEQN')
recall_1_I_['CYCLE'] = '15_16'
recall_1_I_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_I_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_I_['DMDBORN'].replace({1 : "US", 2 : "Elsewhere", 77 : 'Unknown', 99 : 'Unknown'}, inplace=True)
recall_1_I_['DR2_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  4 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  7 : 'Drink',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  18 : 'Snack',
  19 : 'Drink',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

demo_J = demo_J[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'DMDBORN4', 'INDFMPIR', 'DMDYRSUS', 'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA']].rename(columns={'DMDBORN4':'DMDBORN'})
iff_1_J = iff_1_J[['SEQN', 'DR1DRSTZ', 'DR1ILINE', 'DR1IFDCD', 'DR1_030Z', 'WTDRD1', 'WTDR2D', 'DR1IGRMS', 'DR1IMOIS', 'DR1IKCAL', 'DR1ICARB', 'DR1ISUGR', 'DR1IFIBE']].rename(columns={'DR1IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IMOIS':'DR2IMOIS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_1_J['diet_day'] = 1
iff_2_J = iff_2_J[['SEQN', 'DR2DRSTZ', 'DR2ILINE', 'DR2IFDCD', 'DR2_030Z', 'WTDRD1', 'WTDR2D', 'DR2IGRMS', 'DR2IMOIS', 'DR2IKCAL', 'DR2ICARB', 'DR2ISUGR', 'DR2IFIBE']].rename(columns={'DR2IFDCD':'DRXFDCD', 'DR1DRSTZ':'DR2DRSTZ', 'DR1ILINE':'DR2ILINE', 'DR1IFDCD':'DR2IFDCD', 'DR1_030Z':'DR2_030Z', 'DR1IGRMS':'DR2IGRMS', 'DR1IKCAL':'DR2IKCAL', 'DR1ICARB':'DR2ICARB', 'DR1ISUGR':'DR2ISUGR', 'DR1IFIBE':'DR2IFIBE'})
iff_2_J['DR2ILINE'] = iff_2_J['DR2ILINE'] + 100 # combining two days of diet recalls, need unique DR Line items for ingredientization; add 100 to each line item for 2nd recalls for unique values
iff_2_J['diet_day'] = 2
iff_J = pd.concat([iff_1_J, iff_2_J])
iff_J['diet_wts'] = np.where(iff_J['WTDR2D'].notna(), iff_J['WTDR2D'], iff_J['WTDRD1'])
iff_J = iff_J.drop(columns=['WTDRD1', 'WTDR2D'])
food_desc_J.drop(columns='DRXFCSD', inplace=True)
recall_1_J = pd.merge(food_desc_J, iff_J, on = 'DRXFDCD')
recall_1_J_ = pd.merge(recall_1_J, demo_J, on = 'SEQN')
recall_1_J_['CYCLE'] = '17_18'
recall_1_J_['RIAGENDR'].replace({1 : 'Male', 2 : 'Female'}, inplace=True)
recall_1_J_['RIDRETH1'].replace({1 : "Mexican_American", 2 : "Other_Hispanic", 3 : "Non-Hispanic_White", 4 : 'Non-Hispanic_Black', 5 : "Other_Multi-Racial"}, inplace=True)
recall_1_J_['DMDBORN'].replace({1 : "US", 2 : "Elsewhere", 77 : 'Unknown', 99 : 'Unknown'}, inplace=True)
recall_1_J_['DR2_030Z'].replace({
  1 : 'Breakfast',
  2 : 'Lunch',
  3 : 'Dinner',
  4 : 'Dinner',
  5 : 'Brunch',
  6 : 'Snack',
  7 : 'Drink',
  8 : 'Infant feeding',
  9 : 'Extended consumption',
  10 : 'Breakfast',
  11 : 'Breakfast',
  12 : 'Lunch',
  13 : 'Snack',
  14 : 'Dinner',
  15 : 'Snack',
  16 : 'Snack',
  17 : 'Snack',
  18 : 'Snack',
  19 : 'Drink',
  91 : 'Unknown',
  99 : 'Unknown'
}, inplace=True)

print('Finished wrangling data; combine data and crosswalk') 
WWEIA_ALL = pd.concat([recall_1_B_, recall_1_C_, recall_1_D_, recall_1_E_, recall_1_F_, recall_1_G_, recall_1_H_, recall_1_I_, recall_1_J_])
WWEIA_ALL = WWEIA_ALL[WWEIA_ALL['DR2DRSTZ']==1]

WWEIA_ALL_DIET = WWEIA_ALL.dropna(subset='DR2IGRMS')

# for FNDDS 09-18 we need to use a crosswalk to account for changes in food codes across cycles
# apply, combine for each cycle to correctly crosswalk codes
xwalk_FG = pd.read_csv('../data/04/fndds_crosswalk/fndds_0910_1112_crosswalk.csv')
xwalk_GH = pd.read_csv('../data/04/fndds_crosswalk/fndds_1112_1314_crosswalk.csv')
xwalk_HI = pd.read_csv('../data/04/fndds_crosswalk/fndds_1314_1516_crosswalk.csv')
xwalk_IJ = pd.read_csv('../data/04/fndds_crosswalk/fndds_1516_1718_crosswalk.csv')

xwalk_F_J = pd.concat([xwalk_FG, xwalk_GH, xwalk_HI, xwalk_IJ])

xwalk_F_J.drop_duplicates(subset='DRXFDCD', inplace=True)

# merge with cross walk to update foodcodes
wweia_xwalk = pd.merge(WWEIA_ALL_DIET, xwalk_F_J, on='DRXFDCD', how = 'left')

wweia_xwalk.drop(columns=['foodcode'],inplace=True)

wweia_xwalk.rename(columns={'DRXFDCD': 'foodcode'},inplace=True)

# string matched / manually updated discontined foodcodes
discon_update = pd.read_csv('../data/03/manually_curated/string_match_discontinued_complete.csv')
discon_update.rename(columns={'DRXFDCD': 'foodcode'},inplace=True)

code_dict = dict(zip(discon_update.foodcode, discon_update.parent_foodcode))
desc_dict = dict(zip(discon_update.DRXFCLD, discon_update.parent_desc))

wweia_fc = wweia_xwalk.replace({'foodcode':code_dict})
wweia_fc = wweia_fc.replace({'DRXFCLD':desc_dict})

# split metadata for combining with averaged recalls in next step
metadata = wweia_fc.drop_duplicates(subset='SEQN')
metadata = metadata[['SEQN', 'RIAGENDR', 'RIDAGEYR', 'RIDRETH1', 'INDFMPIR',
       'DMDEDUC3', 'DMDEDUC2', 'WTINT2YR', 'WTMEC2YR', 'SDMVPSU', 'SDMVSTRA',
       'CYCLE', 'diet_wts']]


# get unique foodcodes to create 1-1 foodcode to food description
codes = wweia_fc.drop_duplicates(subset='foodcode')
codes = codes[['foodcode', 'DRXFCLD']]
wweia_fc.drop(columns=['DRXFCLD'],inplace=True)
wweia_fc_ = wweia_fc.merge(codes, on='foodcode', how='left')

# average intake over 2 diet recall days
# sum intakes
recalls_sum = wweia_fc_.groupby(['SEQN', 'diet_day', 'foodcode', 'DRXFCLD'])[['DR2IGRMS', 'DR2IKCAL', 'DR2IMOIS']].agg(np.sum).reset_index()
recalls_sum.set_index(['SEQN', 'diet_day', 'foodcode', 'DRXFCLD'],inplace=True)
r_sum = recalls_sum.unstack(level=['diet_day'], fill_value=0).stack()
r_sum.reset_index(inplace=True)

# average intakes
recalls_mean = r_sum.groupby(['SEQN', 'foodcode', 'DRXFCLD'])[['DR2IGRMS', 'DR2IKCAL', 'DR2IMOIS']].mean().reset_index()

# combine with metadata
recalls_mean_meta = recalls_mean.merge(metadata, on='SEQN', how='left')

# Save dataset
recalls_mean_meta.to_csv('../data/04/wweia_foodcodes.txt', sep = '\t', index=None)