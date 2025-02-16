-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. Remove all Duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove Any Columns or Rows

-- Its not good to work on raw data, lets create a similar one
-- Now lets create a new table where we can work on that table, lets copy and paste with different name

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

 -- Insert the same data from layoffs table
 
 INSERT layoffs_staging
 SELECT *
 FROM layoffs;

-- lets create a row number first to check duplicate

SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- lets find out the duplicate, if there row_num is 2 or above then its duplicate

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


-- its not possible to delete the rows having row_num is more than 1, so we will create a new table here


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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- lets delete duplicates

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- STANDARDIZING DATA :
-- it means finding issue in our data and fix it

SELECT
	company,
    TRIM(company)
FROM layoffs_staging2;

-- lets update company column
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Lets fix industry now

 SELECT *
 FROM layoffs_staging2
 WHERE industry LIKE 'Crypto%';
 
 -- Update all crypto industry name as Crypto
 
 UPDATE layoffs_staging2
 SET industry = 'Crypto'
 WHERE industry LIKE 'Crypto%';
 
 SELECT DISTINCT industry
 FROM layoffs_staging2;
 
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- there is issue with united states, lets fix this

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- lets change the date format as this is in text format

SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE (`date`, '%m/%d/%Y');

-- but still the date column will be the same as text in format,
-- so lets alter that column in date format

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; 

-- lets check

SELECT *
FROM layoffs_staging2;

-- lets look for industry

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- we can see that few of the industies are black, but thats not for our data output,
-- lets check for companies data what industry are they belong to

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- from here we can see that Airbnb belongs from travel industry, so we fix those NULL or no value data with travel

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- lets update

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- lets look

SELECT *
FROM layoffs_staging2;

-- lets delete all the data who has NULL in both column total laid off and percentage laid off

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

-- Now lets delete the row_num column

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- THE END





























