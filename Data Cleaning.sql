-- Data Cleaning -------------------------------------------------------------------------------------------------------------------------------
USE world_layoffs;

SELECT * 
FROM layoffs;

-- Creating another table like layoffs not to edit in the original table -------------------------------------------------------------------------
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT * 
FROM layoffs_staging;

-- 1. Removing Duplicates ------------------------------------------------------------------------------------------------------------------------------

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Checking one of the duplicates
SELECT *
FROM layoffs_staging
WHERE company='Yahoo';


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- 2. Standardize Data ------------------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT company
FROM layoffs_staging2;

-- We can see spaces left side for company we need to trim them
SELECT Company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2;

-- To check the industry column

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- In industry column we can see the Crypto, Crypto Currency, CryptoCurrency are three distinct industry. We need to make them as one industry changing them into Crypto
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- To Check the country column

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- In country column United States & United States. are distinct we need to fix this by removing the dot

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Changing date column which is in text type into date type
SELECT date,
STR_TO_DATE(date, '%m/%d/%Y')
FROM layoffs_staging2; 

-- Now we will change the date column into date format first then we will change the date column type text into date
UPDATE layoffs_staging2
SET date = STR_TO_DATE(date, '%m/%d/%Y');

SELECT date
FROM layoffs_staging2; 

ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE;

-- 3. Removing Null values------------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT COUNT(*) AS null_count
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND
percentage_laid_off IS NULL;

SELECT COUNT(*) AS null_count
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND
percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry;

CREATE PROCEDURE count_total_null_values()
SELECT
     COUNT(*) AS total_rows,
     SUM(CASE WHEN company IS NULL THEN 1 ELSE 0 END) AS null_company,
     SUM(CASE WHEN location IS NULL THEN 1 ELSE 0 END) AS null_location,
     SUM(CASE WHEN industry IS NULL THEN 1 ELSE 0 END) AS null_industry,
     SUM(CASE WHEN total_laid_off IS NULL THEN 1 ELSE 0 END) AS null_total_laid_off,
     SUM(CASE WHEN percentage_laid_off IS NULL THEN 1 ELSE 0 END) AS null_percentage_laid_off,
     SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS null_date,
     SUM(CASE WHEN stage IS NULL THEN 1 ELSE 0 END) AS null_stage,
	 SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END) AS null_country,
     SUM(CASE WHEN funds_raised_millions IS NULL THEN 1 ELSE 0 END) AS null_funds_raised_millions
FROM layoffs_staging2;

call count_total_null_values();
     
-- From above noticed that industry column has only one Null lets try to fix this

SELECT *
FROM layoffs_staging2
WHERE (industry IS NULL ) OR (industry = '');

-- We will try to fill industry null values with seeing other rows with other industry values details
SELECT *
FROM layoffs_staging2
WHERE
     (company='Airbnb') OR
     (company="Bally's Interactive") OR
     (company='Carvana') OR
     (company='Juul');



UPDATE layoffs_staging2
SET industry=
    CASE
    WHEN (company='Airbnb') AND ((industry IS NULL) OR (industry=''))
    THEN 'Travel'
    WHEN (company='Carvana') AND ((industry IS NULL) OR (industry=''))
    THEN 'Transportation'
	WHEN (company='Juul') AND ((industry IS NULL) OR (industry=''))
    THEN 'Consumer'
    ELSE industry
    END;
    
call count_total_null_values();

SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE (stage IS NULL) OR (stage='Unknown');

UPDATE layoffs_staging2
SET stage = NULL
WHERE stage = 'Unknown'; 

SELECT stage
FROM layoffs_staging2;

SELECT COUNT(*)
FROM layoffs_staging2
WHERE (total_laid_off IS NULL) AND
	  (percentage_laid_off IS NULL) AND
      (stage IS NULL) AND
      (funds_raised_millions IS NULL);
       

-- DELETE
-- FROM layoffs_staging2
-- WHERE (total_laid_off IS NULL) OR
-- 	  (percentage_laid_off IS NULL) OR
--       (stage IS NULL) OR
--       (funds_raised_millions IS NULL) OR 
--       (industry IS NULL) OR
--       (date IS NULL);

-- Deleteing the null data where only total_laid_off and percentage_laid_off is null


DELETE
FROM layoffs_staging2
WHERE (total_laid_off IS NULL) AND
	  (percentage_laid_off IS NULL) ;

call count_total_null_values();

-- 4.Droppping row_num column ------------------------------------------------------------------------------------------------------------------------------
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;





