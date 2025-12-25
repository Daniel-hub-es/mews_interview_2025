# Mews Reservation Analysis Project 🏨

This repository contains an end-to-end analytics engineering and analytics solution for the Mews Hotel Reservation Task. The project demonstrates the usage of **dbt (Data Build Tool)** pipeline and **Power Bi** dashboarding skills.

## 🚀 Project Overview

The objective is to analyze and answer the questions from the Task provided using reservation patterns, guest behavior, and profit. 

### Key Business Questions Addressed:
1) What are the popular choices of booking rates (table `rate`, columns `ShortRateName` or `RateName`) for different segments of customers (table `reservation`, columns `AgeGroup`, `Gender`, `NationalityCode`)?

2) What are the typical guests who do online check-in? Is it somehow different when you compare reservations created across different weekdays (table `reservation`, `IsOnlineCheckin` column)?

3) Look at the average night revenue per single occupied capacity. What guest segment is the most profitable per occupied space unit? And what guest segment is the least profitable?

4) Bonus: As a bonus assignment, we want to motivate our hotels to promote online checkin and we want to give them some hard data. Look at the data and your analysis again and estimate what would be the impact on total room revenue if the overall usage of online checkin doubled.  


---

## 🛠 Stack

- **Database:** PostgreSQL (Containerized via Docker)
- **Transformation:** dbt (Data Build Tool)
- **External data added:** Jupyter Notebooks (Python/Pandas)
- **Environment:** Python .env, Docker & Docker Compose

---

## 📂 Repository Structure

- `/mews_project`: The core dbt project.
  - `models/staging`: Data cleaning, renaming, and casting.
  - `models/intermediate`: Joins and temporal logic (denormalization of the data tables provided).
  - `models/marts`: Final Fact and Dimension tables to answer task questions.
- `/jupyter_notebooks`: Creation of auxiliar calendar table to show python usage.
- `docker-compose.yml`: Orchestration for the PostgreSQL database and PgAdmin UI.
- `requirements.txt`: Python dependencies for the notebook environment.

---

## 🏗 Data Architecture

This project follows the **Medallion Architecture** to ensure data quality.

---

## ⚙️ Setup & Execution

### 1. Database & Environment
Set up your .env file and add the credentials to access the database
```env
# env file for PostgreSQL and pgAdmin configuration

#  PostgreSQL settings
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=

#  pgAdmin settings
PGADMIN_EMAIL=
PGADMIN_PASSWORD=
```
Start the PostgreSQL container:
```bash
docker-compose up -d
```
Access to your PgAdmin UI
<img width="1664" height="813" alt="image" src="https://github.com/user-attachments/assets/c1d86fd7-7812-4cf7-bc04-9d587bce36db" />

Add the postgresql server
<img width="1716" height="829" alt="image" src="https://github.com/user-attachments/assets/86202cb5-a32c-4fe3-88c7-cdb48721b0af" />
<img width="1713" height="843" alt="image" src="https://github.com/user-attachments/assets/2eec9b68-0e32-4f07-a062-b4f1055de35d" />

### 2. Launch project

Create a python virtual environment and activate the virtual environment:
```bash
pyton3 -m venv '.venv'

. .venv/bin/activate
```
Install dependencies and confirm dbt installation:
```
pip install -r requirements.txt

dbt --version
```
Add the profiles.yml file into your .dbt folder:
```yml
mews_project:
  outputs:
    dev:
      dbname: mydatabase
      host: localhost
      pass: secret
      port: 5432
      schema: public
      threads: 16
      type: postgres
      user: admin
  target: dev
```
Navigate to the folder of the project `mews_project` and run `dbt debug` to ensure connection.

Build the entire project to load `seeds`, `snapshots`, `models` and `tests` in **DAG** order:
```bash
dbt seed
```
```bash
dbt build
```

---

## 📊 Analysis

### 1. SQL ad-hoc queries

#### 1) What are the popular choices of booking rates (table `rate`, columns `ShortRateName` or `RateName`) for different segments of customers (table `reservation`, columns `AgeGroup`, `Gender`, `NationalityCode`)?

For this analysis, we have created a fct table called `fct__rate_popularity` from the intermediate model `int__reservations_rates` where we have joined the tables `rate` and `reservation` as a denormalization process to avoid multiple joins in the **mart** layer.

```sql
select *
from fct__rate_popularity
limit 20;
```
The sql logic to build the mart:

```sql
with 

    reservations_rates as (
        select *
        from {{ ref('int__reservations_rates') }}
    ),
    
    rate_popularity as (
    select
        -- Dimensions
        rate_name, 
        age_group,
        gender, 
        nationality_code,
        -- Number of records
        count(*) as total_reservations
    from reservations_rates
    group by
        -- group by clause for the aggregation
        rate_name, 
        age_group,
        gender, 
        nationality_code
        -- order records from the greatest to the lowest amount
    order by total_reservations desc
    )

select * from rate_popularity
```

The following [ad-hoc sql queries](./mews_project/analyses/key_question_one) shows the popular **booking rates** for the diferent dimensions (`age_group`, `gender` and `nationality_code`). 

#### Popular booking rates by gender:

| rate_name      | gender | booking_rate_volume |
|----------------|--------|---------------------|
| Fully Flexible | 0      | 8                   |
| Fully Flexible | 1      | 156                 |
| Fully Flexible | 2      | 73                  |

#### Popular booking rates by age_group:

| rate_name       | age_group | booking_rate_volume |
|-----------------|-----------|---------------------|
| Fully Flexible  | 0         | 57                  |
| Fully Flexible  | 25        | 41                  |
| Fully Flexible  | 35        | 43                  |
| Fully Flexible  | 45        | 46                  |
| Fully Flexible  | 55        | 34                  |
| Fully Flexible  | 65        | 14                  |
| Early - 21 days | 100       | 3                   |

#### Popular booking rates by nationality (avoiding `NULL` records):

| rate_name             | nationality_code | booking_rate_volume |
|-----------------------|------------------|---------------------|
| Fully Flexible        | AE               | 2                   |
| Min 3 nights          | AL               | 1                   |
| Fully Flexible        | AR               | 2                   |
| Fully Flexible        | AS               | 1                   |
| Fully Flexible        | AT               | 4                   |
| Fully Flexible        | AU               | 5                   |
| Fully Flexible        | BE               | 5                   |
| Fully Flexible        | BG               | 1                   |
| Early - 60 days       | BH               | 1                   |
| Fully Flexible        | BR               | 3                   |
| Fully Flexible        | BY               | 2                   |
| Fully Flexible        | CA               | 7                   |
| Fully Flexible        | CH               | 6                   |
| Fully Flexible        | CL               | 1                   |
| Early - 21 days       | CN               | 8                   |
| Min 4 nights          | CR               | 1                   |
| Fully Flexible        | CZ               | 9                   |
| Fully Flexible        | DE               | 11                  |
| Fully Flexible        | DK               | 5                   |
| Fully Flexible        | DO               | 2                   |
| Fully Flexible        | EE               | 1                   |
| Non Refundable BAR BB | ES               | 3                   |
| Fully Flexible        | FI               | 1                   |
| Fully Flexible        | FR               | 10                  |
| Fully Flexible        | GB               | 13                  |
| Min 3 nights          | GR               | 1                   |
| Fully Flexible        | HK               | 3                   |
| Fully Flexible        | HU               | 1                   |
| Fully Flexible        | ID               | 1                   |
| Fully Flexible        | IE               | 5                   |
| Fully Flexible        | IL               | 6                   |
| Fully Flexible        | IN               | 3                   |
| Fully Flexible        | IQ               | 1                   |
| Min 3 nights          | IR               | 1                   |
| Fully Flexible        | IS               | 1                   |
| Fully Flexible        | IT               | 9                   |
| Fully Flexible        | JO               | 1                   |
| Fully Flexible        | JP               | 4                   |
| Fully Flexible        | KR               | 5                   |
| Min 3 nights          | KW               | 2                   |
| Fully Flexible        | LB               | 2                   |
| Non Refundable BAR BB | LT               | 1                   |
| Fully Flexible        | LU               | 3                   |
| Fully Flexible        | MC               | 1                   |
| Fully Flexible        | MT               | 1                   |
| Fully Flexible        | MX               | 6                   |
| Non Refundable BAR BB | MY               | 1                   |
| Fully Flexible        | NL               | 7                   |
| Fully Flexible        | NO               | 3                   |
| Fully Flexible        | NZ               | 3                   |
| Fully Flexible        | PE               | 1                   |
| Early - 21 days       | PH               | 2                   |
| Fully Flexible        | PL               | 3                   |
| Fully Flexible        | PR               | 1                   |
| Fully Flexible        | PT               | 1                   |
| Min 4 nights          | QA               | 2                   |
| Fully Flexible        | RO               | 1                   |
| Fully Flexible        | RU               | 7                   |
| Fully Flexible        | SA               | 3                   |
| Fully Flexible        | SC               | 1                   |
| Fully Flexible        | SE               | 6                   |
| Early - 21 days       | SG               | 3                   |
| Fully Flexible        | SK               | 10                  |
| Min 3 nights          | SY               | 1                   |
| Fully Flexible        | TH               | 3                   |
| Fully Flexible        | TJ               | 1                   |
| Fully Flexible        | TR               | 3                   |
| Fully Flexible        | TW               | 2                   |
| Fully Flexible        | UA               | 2                   |
| Fully Flexible        | US               | 16                  |

---

#### 2) What are the typical guests who do online check-in? Is it somehow different when you compare reservations created across different weekdays (table `reservation`, `IsOnlineCheckin` column)?

For this analysis, we have created a fct table called `fct__online_checkins` from the intermediate model `int__reservations_rates` joining it with `dim__calendar` table be able to make the comparasion across different weekdays in the **mart layer**:

```sql
select *
from fct__online_checkins
limit 20;
```

The sql logic to build the mart:

```sql
with
  -- Intermediate model (rates + reservations)
	base as (
		select *
		from  {{ ref('int__reservations_rates') }}
	),
  -- aux dimensional calendar table with days of the week
	calendar as (
		select *
		from {{ref('dim__calendar')}}
	),

	final as (
		select
      -- date and weekday
			base.created_utc,
			cal.weekday as created_day,
      -- dimensions
			base.age_group,
			base.gender,
			base.nationality_code,
			base.rate_name,
			count(*) as total_reservations, -- amoubnt of reservations
			sum(is_online_checkin) as total_online_checkin --amount of reservations that are online
		from base
    -- join weekdays by date with base table
		left join calendar cal
		on cast(base.created_utc as date) = cal.dates
		group by
      -- group by clause for the aggregation
			base.created_utc,
			cal.weekday,	
			base.age_group,
			base.gender,
			base.nationality_code,
			base.rate_name
    -- order records from the greatest to the lowest amount
		order by total_online_checkin desc
	)

select * from final
```

The following [ad-hoc sql queries](./mews_project/analyses/key_question_two) shows the guests and the amount of online checkins and the amount of online checkings per weekday (`age_group`, `gender` and `nationality_code`). 

#### Amount of reservations and online checkins by dimension:

| dimension_type   | value | amount_of_reservations | amount_of_online_checkin |
|------------------|-------|------------------------|--------------------------|
| age_group        | 35    | 279                    | 47                       |
| gender           | 1     | 1295                   | 119                      |
| nationality_code | US    | 243                    | 25                       |

#### Amount of reservations and online checkins by `age_group` per weekday:

| created_day | dimension_type | value | amount_of_reservations | amount_of_online_checkin |
|-------------|----------------|-------|------------------------|--------------------------|
| Friday      | age_group      | 45    | 35                     | 6                        |
| Monday      | age_group      | 35    | 51                     | 11                       |
| Saturday    | age_group      | 25    | 24                     | 6                        |
| Sunday      | age_group      | 55    | 14                     | 4                        |
| Thursday    | age_group      | 35    | 49                     | 9                        |
| Tuesday     | age_group      | 35    | 53                     | 9                        |
| Wednesday   | age_group      | 55    | 20                     | 6                        |

#### Amount of reservations and online checkins by `gender` per weekday:

| created_day | dimension_type | value | amount_of_reservations | amount_of_online_checkin |
|-------------|----------------|-------|------------------------|--------------------------|
| Friday      | gender         | 1     | 163                    | 15                       |
| Monday      | gender         | 1     | 239                    | 19                       |
| Saturday    | gender         | 1     | 122                    | 14                       |
| Sunday      | gender         | 1     | 115                    | 12                       |
| Thursday    | gender         | 1     | 228                    | 18                       |
| Tuesday     | gender         | 1     | 209                    | 21                       |
| Wednesday   | gender         | 1     | 219                    | 20                       |

#### Amount of reservations and online checkins by `nationality` per weekday:

| created_day | dimension_type   | value | amount_of_reservations | amount_of_online_checkin |
|-------------|------------------|-------|------------------------|--------------------------|
| Friday      | nationality_code | RU    | 9                      | 3                        |
| Monday      | nationality_code | GB    | 42                     | 6                        |
| Saturday    | nationality_code | US    | 29                     | 5                        |
| Sunday      | nationality_code | GB    | 14                     | 2                        |
| Thursday    | nationality_code | SE    | 6                      | 3                        |
| Tuesday     | nationality_code | US    | 34                     | 5                        |
| Wednesday   | nationality_code | US    | 33                     | 5                        |

---

#### 3) Look at the average night revenue per single occupied capacity. What guest segment is the most profitable per occupied space unit? And what guest segment is the least profitable?

For this analysis, we have created a fct table called `fct__revenue` from the intermediate model `int__reservations_rates` to know wich is the most and least profitable guest segment pero occupied space unit in **mart layer**:

```sql
select *
from fct__revenue
limit 20;
```

The sql logic to build the mart:

```sql
with 

    base as (
        select *
        from  {{ ref('int__reservations_rates') }}
    ),

    final as (
        select
            created_utc,
			-- Dimensions
            age_group,
            gender,
            nationality_code,
            rate_name,
            night_count,
            night_cost_sum,
			-- Measures for the calculation
            occupied_space_sum,
            guest_count_sum,
			-- Normalize the revenue by dividing the total cost by the capacity units used
			-- Case statements prevent divisions by zero
            case 
                when occupied_space_sum > 0 
                    then night_cost_sum / occupied_space_sum 
                else 0 
            end as rev_per_capacity

        from base
        order by rev_per_capacity desc
    )

select * from final
```

The following sql query shows the `age_group` that is most profitable and least profitable:

```sql
select
	age_group,
	max(rev_per_capacity) as max_rev_per_capacity
from fct__revenue
group by age_group
order by max_rev_per_capacity desc
```

The next sql query shows the `gender` that is most profitable and least profitable:

```sql
select
	gender,
	max(rev_per_capacity) as max_rev_per_capacity
from fct__revenue
group by gender
order by max_rev_per_capacity desc
```

The last sql query shows the `nationality_code` that is most profitable and least profitable:

```sql
select
	nationality_code,
	max(rev_per_capacity) as max_rev_per_capacity
from fct__revenue
where nationality_code is not null
group by nationality_code
order by max_rev_per_capacity desc
```

---

#### 4) Bonus: As a bonus assignment, we want to motivate our hotels to promote online checkin and we want to give them some hard data. Look at the data and your analysis again and estimate what would be the impact on total room revenue if the overall usage of online checkin doubled.

For this analysis, we have created a fct table called `fct__checkins_growth` from the intermediate model `int__reservations_rates` and create calculations to show actual revenua and possible growth on revenue based on the growth of online checkins in the **mart layer**:

```sql
select *
from fct__checkins_growth;
```

The sql logic to build the mart:

```sql
with

	base as (
		select *
		from {{ ref('int__reservations_rates') }}
	),

	-- the following CTE creates two rows of data (online and desk checks)
	metrics as (
		select 
			is_online_checkin,
			count(*) as total_bookings,
			sum(night_cost_sum) as total_night_cost_sum,
			-- shows if digital guests spend more on average than on-site guests
			avg(night_cost_sum) as avg_night_cost_sum
		from base
		group by 1
	),

	proposal_metrics as (
		select
				-- case check if the group of the resulting rows is an online guest or on-site guests
				-- collapsing into one single row by max() by column when the other result is 0 
		        max(case 
						when is_online_checkin = 1 
						then total_bookings 
						else 0 
					end) as online_bookings,
		        max(case 
						when is_online_checkin = 1 
						then avg_night_cost_sum 
						else 0 
					end) as avg_online_rev,
		        max(case 
						when is_online_checkin = 0 
						then avg_night_cost_sum 
						else 0 
					end) as avg_offline_rev,
				-- total revenue and global total bookings to compare in the final calculations
		        sum(total_night_cost_sum) as total_revenue,
		        sum(total_bookings) as global_total_bookings
		    from metrics			
	),

	calculated_diff as (
		select
			-- metrics
			global_total_bookings,
			online_bookings,
			avg_online_rev,
			avg_offline_rev,
			-- calculate the difference between online and ofline rev
			(avg_online_rev - avg_offline_rev) as avg_revenue_diff,
			-- total current revenue
			total_revenue as current_total_revenue,
			-- the incremental gain by bookings * extra value per booking calculation to show extra revenue
			online_bookings * (avg_online_rev - avg_offline_rev) as online_projected_growth_revenue,
			-- percentage revenue growth calculation using online_projected_growth_revenue calculated before
			round(
				((online_bookings * (avg_online_rev - avg_offline_rev) / total_revenue) * 100), 2
			) as percentage_revenue_growth,
			-- sum existing revenue and the extra revenue from moving from lower spender to higher spender (online checkins)
			total_revenue + (online_bookings * (avg_online_rev - avg_offline_rev)) as projected_total_revenue
		from proposal_metrics
	)
	
select * from calculated_diff
```
