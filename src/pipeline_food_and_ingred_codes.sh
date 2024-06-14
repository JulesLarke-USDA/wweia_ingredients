#!/bin/bash

echo "Initializing step 1: consolidating ingredient codes"; date
python3 ../lib/01_consolidate_codes.py
echo "Initializing step 2: calculating ingredient weights"; date
python3 ../lib/02_calculate_ingredient_weights.py
echo "Initializing step 3: text matching discontinued codes"; date
python3 ../lib/03_text_match_discontinued_codes.py
echo "Initializing step 3b: Further ingredientizing with FCID"; date
python3 ../lib/03b_fcid_ingredients.py
echo "Initializing step 6: Combining WWEIA foodcode and ingredient code data"; date
python3 ../lib/06_wweia_food_matched_ingred_codes.py
echo "Success! Pipeline finished on:"; date
