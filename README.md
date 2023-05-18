# WWEIA Ingredients

The field of nutritional epidemiology attemps to link dietary intake data with health outcomes or other biomarkers associated with health or disease. However, dietary data from current publicly available databases is often aggregated at the level of food groups or mixed dishes, thereby limiting resolution and analysis of individual ingredients that may contribute to an outcome of interest. 

Recognizing these challenges, this project modifies the National Health And Nutrition Examination Survey (NHANES) What We Eat In America (WWEIA) data from 2001-2018 to generate an 'ingredientized' dataset by updating food codes and descriptions with the Food and Nutrition Database for Dietary Studies (FNDDS) database versions 2015-2016 and 2017-2018.

## Workflow

### 01: Consolidate ingredient codes
 
__Purpose__  
To reduce the number of ingredients used by averaging commonly consumed food items. This creates a single averaged ingredient code from the multiple codes and weights found in these commonly consumed ingredients and reduces over-complexity and redundany.

__Required Input Files__

  - **fndds2016.csv** - Downloaded from: https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2015-2016%20FNDDS%20At%20A%20Glance%20-%20FNDDS%20Ingredients.xlsx
  - **fndds2018.csv** - Downloaded from: https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2017-2018%20FNDDS%20At%20A%20Glance%20-%20FNDDS%20Ingredients.xlsx

__Output__
- **fndds_16_consolidated_ingredient_codes_050523.csv**
- **fndds_18_consolidated_ingredient_codes_050523.csv**

### 02: Formatting ingredient codes

__Purpose__  
To remap ingredient codes represented as 8-digit foodcodes for obtaining proper ingredient weights per ingredient code

__Required Input Files__

  - **fndds_16_consolidated_ingredient_codes_050523.csv** - Output from 01_consolidate_ingredient_codes.ipynb
  - **fndds_18_consolidated_ingredient_codes_050523.csv** - Output from 01_consolidate_ingredient_codes.ipynb

__Output__
- **fndds_16_wts_to_correct_050523.csv**
- **fndds_18_wts_to_correct_050523.csv**

### 03: Combine ingredient codes

__Purpose__  
To update the ingredient codes with averages of ingredients to limit number of additional ingredient descriptions and will help with ingredient tree representation. 

__Required Input Files__

  - **fndds_2016_ingredient_nutrient_values.csv** - Downloaded from: https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2015-2016%20FNDDS%20At%20A%20Glance%20-%20Ingredient%20Nutrient%20Values.xlsx
  - **fndds_2018_ingredient_nutrient_values.csv** - Downloaded from: https://www.ars.usda.gov/ARSUserFiles/80400530/apps/2017-2018%20FNDDS%20At%20A%20Glance%20-%20FNDDS%20Nutrient%20Values.xlsx

__Output__
- **fndds_15_18_all_ingredient_nutrient_vals.csv** (Used in 06_wweia_ingredients.py)

### 04: Calculate FNDDS Ingredient Weights

__Purpose__  
This script recursively calculates the ingredient weight amounts within 8-digit foodcodes. This is needed to correctly represent the relative proportions of ingredient codes using 8-digit foodcodes

__Required Input Files__

  - **fndds_16_wts_to_correct_050523.csv** - Output from fndds_16_get_ingredient_weights.R
  - **fndds_18_wts_to_correct_050523.csv** - Output from fndds_18_get_ingredient_weights.R
  - **fndds_16_consolidated_ingredient_codes_050523.csv** - Output from 01_fndds_consolidate_ingredient_codes.ipynb
  - **fndds_16_consolidated_ingredient_codes_050523.csv** - Output from 01_fndds_consolidate_ingredient_codes.ipynb

__Workflow__
    1) Recursion: preprocessing to format dataframe for the get_ingredient_proportions function and then perform calucations with get_ingredient_proportions
    2) Combine result with the remianing ingredient codes that were properly represented as ingredient codes and not foodcodes in fndds_XX_consolidated_ingredient_codes_050523.csv 
    3) Combine the results for FNDDS1516 and FNDDS1718 into a single dataframe
    
__Output__
  - **fndds_16_18_all.csv**

### 05: Matching Discontinued FNDDS (01-13) Ingredient Descriptions

__Required Input Files__

  - **wweia_discontinued_foodcodes.csv** - Output from wweia_ingredients.R 
  - **fndds_16_18_all.csv** - Output from fndds_calculate_ingredient_weights.ipynb

__Information__  
This script prepares food descriptions in discontined foodcodes from WWEIA cycles (01 - 13) corresponding to early FNDDS version for downstream text similarity matching. This script achieves the following:
    
    1) Text cleaning: removal of punctuation and stopwords, lemmatization, etc.
    2) Finds matches based on main food descriptions (rather than foodcodes which were used in the crosswalk before) and exports these matches
        
__Output__
  - **string_match.csv** - This file will undergo manual curation to match discontinued foodcodes to the most appropriate foodcode in fndds_16_18_all.csv

__Note:__  The output of this manual matching is: string_match_discontinued_complete.csv and string_match_discontinued_complete_with_annotation.csv for documentation on added foodcodes
Similarly, fndds_16_18_all.csv was updated with new codes for recipes that did not exists and were needed to match discontinued foodcodes. This updated file is: fndds_16_18_all_added_codes_for_discontinued.csv
Both of these manually curated outputs [string_match_discontinued_complete.csv and fndds_16_18_all_added_codes_for_discontinued.csv] are used in the next script: 06_wweia_ingredients.py

### 06: WWEIA Ingredient

__Required Input Files__

  - **fndds_16_18_all_added_codes_for_discontinued.csv** - Output from previous script, manually curated
  - **string_match_discontinued_complete.csv** - Output from previous script, manually curated
  - **fndds_crosswalks** - Each of the crosswalk files in the fndds_crosswalk directory

__Information__  
This script prepares combines data for each of the WWEIA cycles (01 - 18), updates the food codes to those with ingredient codes and appends nutrient values corresponding to the ingredients.

__Output__
  - **wweia_all_recalls.txt** - The complete ingredientized dataset for WWEIA cycles 01-18. Approximately 2.3M rows of diet intake with 65 nutrient estimates for each ingredient across 80275 individuals


