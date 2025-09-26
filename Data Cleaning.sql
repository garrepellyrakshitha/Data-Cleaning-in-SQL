-- Data Cleaning

select * from layoffs;

-- Remove Duplicates
-- Stabndardarize the data
-- Nulls and blanks
-- Remove unnecessary 

-- Remove Duplicates

create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert layoffs_staging
select * from layoffs;

with duplicate_cte as
(
select *,
row_number()
over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) 
as row_num
from layoffs_staging
)
select * 
from duplicate_cte 
where row_num > 1;

select * from layoffs_staging 
where company = 'Casper';

with duplicate_cte as
(
select *,
row_number()
over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) 
as row_num
from layoffs_staging
)
delete 
from duplicate_cte 
where row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2
where row_num > 1;

insert into layoffs_staging2
select *,
row_number()
over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) 
as row_num
from layoffs_staging;

delete from layoffs_staging2
where row_num > 1;

select * from layoffs_staging2;

-- Stardardizing the Data

select company, trim(company)
from layoffs_staging2;


update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country
 from layoffs_staging2
 order by 1;
 
 update layoffs_staging2
 set country = 'United States'
 where country like 'United States%';
 
 -- we can either use this
 -- select distinct country, trim(trailing '.' from country) 
 -- from layoffs_staging2 order by 1;
 
 select `date`, STR_TO_DATE(`date`, '%Y-%m-%d')
 from layoffs_staging2;
 
    UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%Y-%m-%d');
 
 select * from layoffs_staging2 where `date` = 'NULL';
 
 
 alter table layoffs_staging2
 modify column `date` date;
 
 
 -- Nulls and Blanks
 
 select * from layoffs_staging2
 where total_laid_off = 'NULL'
 and percentage_laid_off = 'NULL';
 
 update layoffs_staging2
 set industry = 'NULL'
 where industry = '';
 
 select * from layoffs_staging2
 where industry = 'NULL' or industry = '';
 
 select * from layoffs_staging2
 where company like 'Bally%';
 
 
 select t1.industry,t2.industry from layoffs_staging2 t1
 join layoffs_staging2 t2 
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry = 'NULL' or t1.industry = '')
and t2.industry != 'NULL';
 
update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry = 'NULL'
and t2.industry != 'NULL';

select * from layoffs_staging2;

 select * 
 from layoffs_staging2
 where total_laid_off = 'NULL'
 and percentage_laid_off = 'NULL';
 
 delete 
 from layoffs_staging2
 where total_laid_off = 'NULL'
 and percentage_laid_off = 'NULL';
 
 select * from layoffs_staging2;
 
 alter table layoffs_staging2
 drop column row_num;
 
 update layoffs_staging2
set total_laid_off = null
where total_laid_off = 'NULL';

ALTER TABLE layoffs_staging2 MODIFY total_laid_off int;

update layoffs_staging2
set funds_raised_millions = null
where funds_raised_millions = 'NULL';

ALTER TABLE layoffs_staging2 MODIFY funds_raised_millions int;

update layoffs_staging2
set percentage_laid_off = null
where percentage_laid_off = 'NULL';

update layoffs_staging2
set stage = null
where stage = 'NULL';