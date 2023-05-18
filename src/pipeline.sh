#!/bin/bash

echo "Initializing step 1: consolidating ingredient codes"; date
python3 ../lib/01_consolidate_codes.py
echo "Initializing step 2: formatting ingredient codes"; date
python3 ../lib/02_format_codes.py
echo "Initializing step 3: combining redundant ingredients"; date
python3 ../lib/03_combine_ingredients.py
echo "Initializing step 4: calculating ingredient weights"; date
python3 ../lib/04_calculate_ingredient_weights.py
echo "Initializing step 5: text matching discontinued codes"; date
python3 ../lib/05_text_match_discontinued_codes.py
echo "Initializing step 6: ingredientizing WWEIA data"; date
python3 ../lib/06_wweia_ingredients.py
echo "Pipeline finished on:"; date
