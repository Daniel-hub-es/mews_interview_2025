## 📊 Analysis

### 1. SQL ad-hoc queries

#### 1) What are the popular choices of booking rates (table `rate`, columns `ShortRateName` or `RateName`) for different segments of customers (table `reservation`, columns `AgeGroup`, `Gender`, `NationalityCode`)?

For this analysis, we have created a fct table called `fct__rate_popularity` from the intermediate model `int__reservations_rates` where we have joined the tables `rate` and `reservation` as a denormalization process to avoid multiple joins in the **mart** layer.

The sql logic to build the mart: [`fct__rate_popularity`](https://github.com/Daniel-hub-es/mews_interview_2025/blob/main/mews_project/models/marts/fct/fct__rate_popularity.sql)

```sql
select *
from fct__rate_popularity
limit 20;
```

The following [ad-hoc sql queries](./mews_project/analyses/key_question_one) shows the popular **booking rates** for the diferent dimensions (`age_group`, `gender` and `nationality_code`). 

#### Popular booking rates by gender:

| popular_rate    | gender    | popular_rate_reservations | percentage_pr | total_reservations | percentage_tr |
|-----------------|-----------|---------------------------|---------------|--------------------|---------------|
| Fully Flexible  | female    | 42                        | 6.36          | 360                | 1.68          |
| Fully Flexible  | male      | 207                       | 31.36         | 1295               | 8.28          |
| Early - 60 days | undefined | 411                       | 62.27         | 846                | 16.43         |


#### Popular booking rates by age_group:

| popular_rate    | age_group | popular_rate_reservations | percentage_pr | total_reservations | percentage_tr |
|-----------------|-----------|---------------------------|---------------|--------------------|---------------|
| Early - 60 days | 0         | 411                       | 78.44         | 1520               | 16.43         |
| Fully Flexible  | 25        | 24                        | 4.58          | 234                | 0.96          |
| Fully Flexible  | 35        | 25                        | 4.77          | 279                | 1.00          |
| Fully Flexible  | 45        | 27                        | 5.15          | 241                | 1.08          |
| Fully Flexible  | 55        | 20                        | 3.82          | 146                | 0.80          |
| Fully Flexible  | 65        | 14                        | 2.67          | 65                 | 0.56          |
| Early - 21 days | 100       | 3                         | 0.57          | 16                 | 0.12          |


#### Popular booking rates by nationality:

| popular_rate          | nationality_code | popular_rate_reservations | percentage_pr | total_reservations | percentage_tr |
|-----------------------|------------------|---------------------------|---------------|--------------------|---------------|
| Early - 60 days       | Unknown          | 411                       | 51.12         | 1096               | 16.43         |
| Fully Flexible        | DE               | 54                        | 6.72          | 154                | 2.16          |
| Fully Flexible        | GB               | 44                        | 5.47          | 187                | 1.76          |
| Fully Flexible        | US               | 25                        | 3.11          | 243                | 1.00          |
| Fully Flexible        | CN               | 21                        | 2.61          | 59                 | 0.84          |
| Fully Flexible        | CZ               | 17                        | 2.11          | 67                 | 0.68          |
| Fully Flexible        | RU               | 16                        | 1.99          | 51                 | 0.64          |
| Fully Flexible        | SK               | 14                        | 1.74          | 72                 | 0.56          |
| Fully Flexible        | FR               | 10                        | 1.24          | 46                 | 0.40          |
| Fully Flexible        | IL               | 10                        | 1.24          | 39                 | 0.40          |
| Early - 21 days       | NO               | 9                         | 1.12          | 17                 | 0.36          |
| Fully Flexible        | IT               | 8                         | 1.00          | 48                 | 0.32          |
| Early - 60 days       | TH               | 8                         | 1.00          | 16                 | 0.32          |
| Fully Flexible        | ID               | 7                         | 0.87          | 7                  | 0.28          |
| Fully Flexible        | IE               | 6                         | 0.75          | 10                 | 0.24          |
| Fully Flexible        | NL               | 6                         | 0.75          | 23                 | 0.24          |
| Fully Flexible        | SA               | 6                         | 0.75          | 15                 | 0.24          |
| Non Refundable BAR BB | SE               | 6                         | 0.75          | 25                 | 0.24          |
| Fully Flexible        | PL               | 6                         | 0.75          | 15                 | 0.24          |
| Fully Flexible        | CA               | 6                         | 0.75          | 27                 | 0.24          |
| Fully Flexible        | ES               | 5                         | 0.62          | 18                 | 0.20          |
| Fully Flexible        | AU               | 5                         | 0.62          | 24                 | 0.20          |
| Fully Flexible        | MX               | 5                         | 0.62          | 18                 | 0.20          |
| Fully Flexible        | LU               | 4                         | 0.50          | 6                  | 0.16          |
| Fully Flexible        | AT               | 4                         | 0.50          | 17                 | 0.16          |
| Fully Flexible        | BE               | 4                         | 0.50          | 13                 | 0.16          |
| Fully Flexible        | BR               | 4                         | 0.50          | 11                 | 0.16          |
| Non Refundable BAR BB | CH               | 4                         | 0.50          | 18                 | 0.16          |
| Min 4 nights          | QA               | 4                         | 0.50          | 6                  | 0.16          |
| Fully Flexible        | DK               | 4                         | 0.50          | 13                 | 0.16          |
| Fully Flexible        | PR               | 4                         | 0.50          | 4                  | 0.16          |
| Fully Flexible        | IN               | 4                         | 0.50          | 10                 | 0.16          |
| Fully Flexible        | KW               | 4                         | 0.50          | 8                  | 0.16          |
| Fully Flexible        | AE               | 3                         | 0.37          | 9                  | 0.12          |
| Fully Flexible        | HK               | 3                         | 0.37          | 5                  | 0.12          |
| Fully Flexible        | JP               | 3                         | 0.37          | 11                 | 0.12          |
| Fully Flexible        | KR               | 3                         | 0.37          | 15                 | 0.12          |
| Early - 21 days       | SG               | 3                         | 0.37          | 10                 | 0.12          |
| Fully Flexible        | TR               | 3                         | 0.37          | 4                  | 0.12          |
| Early - 21 days       | PH               | 2                         | 0.25          | 3                  | 0.08          |
| Fully Flexible        | RO               | 2                         | 0.25          | 3                  | 0.08          |
| Fully Flexible        | NZ               | 2                         | 0.25          | 4                  | 0.08          |
| Fully Flexible        | BY               | 2                         | 0.25          | 4                  | 0.08          |
| Fully Flexible        | DO               | 2                         | 0.25          | 2                  | 0.08          |
| Fully Flexible        | LB               | 2                         | 0.25          | 3                  | 0.08          |
| Fully Flexible        | AR               | 2                         | 0.25          | 2                  | 0.08          |
| Non Refundable BAR BB | TW               | 2                         | 0.25          | 7                  | 0.08          |
| Fully Flexible        | UA               | 2                         | 0.25          | 3                  | 0.08          |
| Fully Flexible        | PE               | 1                         | 0.12          | 1                  | 0.04          |
| Fully Flexible        | EE               | 1                         | 0.12          | 1                  | 0.04          |
| Fully Flexible        | PT               | 1                         | 0.12          | 1                  | 0.04          |
| Min 3 nights          | IR               | 1                         | 0.12          | 1                  | 0.04          |
| Fully Flexible        | CL               | 1                         | 0.12          | 1                  | 0.04          |
| Fully Flexible        | SC               | 1                         | 0.12          | 1                  | 0.04          |
| Early - 60 days       | BH               | 1                         | 0.12          | 2                  | 0.04          |
| Non Refundable BAR BB | BG               | 1                         | 0.12          | 2                  | 0.04          |
| Min 3 nights          | SY               | 1                         | 0.12          | 1                  | 0.04          |
| Fully Flexible        | AS               | 1                         | 0.12          | 1                  | 0.04          |
| Fully Flexible        | TJ               | 1                         | 0.12          | 1                  | 0.04          |
| Min 3 nights          | AL               | 1                         | 0.12          | 1                  | 0.04          |
| Fully Flexible        | IS               | 1                         | 0.12          | 2                  | 0.04          |
| Fully Flexible        | JO               | 1                         | 0.12          | 1                  | 0.04          |
| Min 4 nights          | CR               | 1                         | 0.12          | 1                  | 0.04          |
| Non Refundable BAR BB | LT               | 1                         | 0.12          | 1                  | 0.04          |
| Fully Flexible        | IQ               | 1                         | 0.12          | 1                  | 0.04          |
| Fully Flexible        | MC               | 1                         | 0.12          | 1                  | 0.04          |
| Fully Flexible        | MT               | 1                         | 0.12          | 2                  | 0.04          |
| Fully Flexible        | HU               | 1                         | 0.12          | 3                  | 0.04          |
| Fully Flexible        | MY               | 1                         | 0.12          | 3                  | 0.04          |
| Min 3 nights          | GR               | 1                         | 0.12          | 1                  | 0.04          |
| Early - 60 days       | FI               | 1                         | 0.12          | 3                  | 0.04          |


---

#### 2) What are the typical guests who do online check-in? Is it somehow different when you compare reservations created across different weekdays (table `reservation`, `IsOnlineCheckin` column)?

For this analysis, we have created a fct table called `fct__online_checkins` from the intermediate model `int__reservations_rates` joining it with `dim__calendar` table be able to make the comparasion across different weekdays in the **mart layer**:

The sql logic to build the mart: [`fct__online_checkins`](https://github.com/Daniel-hub-es/mews_interview_2025/blob/main/mews_project/models/marts/fct/fct__online_checkins.sql)

```sql
select *
from fct__online_checkins
limit 20;
```

The following [ad-hoc sql queries](./mews_project/analyses/key_question_two) shows the guests and the amount of online checkins and the amount of online checkings per weekday (`age_group`, `gender` and `nationality_code`). 

#### Typical guest who do online checkins:

| age_group | gender | nationality_code | amount_of_online_checkin |
|-----------|--------|------------------|--------------------------|
| 35        | male   | GB               | 8                        |

#### Amount of reservations and online checkins by dimension:

| dimension_type   | value | amount_of_reservations | amount_of_online_checkin | percentage_of_online_per_total_reservations |
|------------------|-------|------------------------|--------------------------|---------------------------------------------|
| age_group        | 35    | 279                    | 47                       | 2.59                                        |
| gender           | male  | 1295                   | 119                      | 6.55                                        |
| nationality_code | US    | 243                    | 25                       | 1.38                                        |


#### Amount of reservations and online checkins by `age_group` per weekday:

| created_day | dimension_type | value | amount_of_reservations | amount_of_online_checkin | percentage_of_online_per_total_reservations |
|-------------|----------------|-------|------------------------|--------------------------|---------------------------------------------|
| Friday      | age_group      | 45    | 35                     | 6                        | 2.26                                        |
| Monday      | age_group      | 35    | 51                     | 11                       | 4.15                                        |
| Saturday    | age_group      | 25    | 24                     | 6                        | 2.26                                        |
| Sunday      | age_group      | 55    | 14                     | 4                        | 1.51                                        |
| Thursday    | age_group      | 35    | 49                     | 9                        | 3.40                                        |
| Tuesday     | age_group      | 35    | 53                     | 9                        | 3.40                                        |
| Wednesday   | age_group      | 35    | 39                     | 6                        | 2.26                                        |


#### Amount of reservations and online checkins by `gender` per weekday:

| created_day | dimension_type | value | amount_of_reservations | amount_of_online_checkin | percentage_of_online_per_total_reservations |
|-------------|----------------|-------|------------------------|--------------------------|---------------------------------------------|
| Friday      | gender         | male  | 163                    | 15                       | 1.16                                        |
| Monday      | gender         | male  | 239                    | 19                       | 1.47                                        |
| Saturday    | gender         | male  | 122                    | 14                       | 1.08                                        |
| Sunday      | gender         | male  | 115                    | 12                       | 0.93                                        |
| Thursday    | gender         | male  | 228                    | 18                       | 1.39                                        |
| Tuesday     | gender         | male  | 209                    | 21                       | 1.62                                        |
| Wednesday   | gender         | male  | 219                    | 20                       | 1.54                                        |


#### Amount of reservations and online checkins by `nationality` per weekday:

| created_day | dimension_type   | value | amount_of_reservations | amount_of_online_checkin | percentage_of_online_per_total_reservations |
|-------------|------------------|-------|------------------------|--------------------------|---------------------------------------------|
| Friday      | nationality_code | RU    | 9                      | 3                        | 1.59                                        |
| Monday      | nationality_code | GB    | 42                     | 6                        | 3.17                                        |
| Saturday    | nationality_code | US    | 29                     | 5                        | 2.65                                        |
| Sunday      | nationality_code | GB    | 14                     | 2                        | 1.06                                        |
| Thursday    | nationality_code | DE    | 28                     | 3                        | 1.59                                        |
| Tuesday     | nationality_code | US    | 34                     | 5                        | 2.65                                        |
| Wednesday   | nationality_code | US    | 33                     | 5                        | 2.65                                        |


---

#### 3) Look at the average night revenue per single occupied capacity. What guest segment is the most profitable per occupied space unit? And what guest segment is the least profitable?

For this analysis, we have created a fct table called `fct__revenue` from the intermediate model `int__reservations_rates` to know wich is the most and least profitable guest segment pero occupied space unit in **mart layer**:

The sql logic to build the mart: [`fct__revenue`](https://github.com/Daniel-hub-es/mews_interview_2025/blob/main/mews_project/models/marts/fct/fct__revenue.sql)

```sql
select *
from fct__revenue
limit 20;
```

The following [ad-hoc sql queries](./mews_project/analyses/key_question_three) shows the most and least profitable general guest and most and least profitable guest by dimensions (`age_group`, `gender` and `nationality_code`). 

#### Typical guesst that are most and least profitable:

| created_utc                | age_group | gender | nationality_code | rate_name             | night_count | night_cost_sum | occupied_space_sum | guest_count_sum | rev_per_capacity       |
|----------------------------|-----------|--------|------------------|-----------------------|-------------|----------------|--------------------|-----------------|------------------------|
| 2019-08-30 17:16:53.597785 | 35        | 2      | GB               | Non Refundable BAR BB | 2           | 2.805924       | 4                  | 2               | 0.70148100000000000000 |
| 2019-10-20 12:13:20.633233 | 0         | 1      | RU               | Fully Flexible        | 1           | 682.112213     | 2                  | 2               | 341.0561065000000000   |

#### Typical guesst that are most and least profitable by age:

| age_group | avg_revenue_per_capacity |
|-----------|--------------------------|
| 55        | 95.8055265713386606      |
| 0         | 42.9521126941445963      |

#### Typical guesst that are most and least profitable by gender:

| gender | avg_revenue_per_capacity |
|--------|--------------------------|
| 2      | 83.93590516426511243500  |
| 0      | 33.3004128418985992      |

#### Typical guesst that are most and least profitable by nationality:

| nationality_code | avg_revenue_per_capacity |
|------------------|--------------------------|
| PT               | 147.0187010000000000     |
| AL               | 36.6134050000000000      |


---

#### 4) Bonus: As a bonus assignment, we want to motivate our hotels to promote online checkin and we want to give them some hard data. Look at the data and your analysis again and estimate what would be the impact on total room revenue if the overall usage of online checkin doubled.

For this analysis, we have created a fct table called `fct__checkins_growth` from the intermediate model `int__reservations_rates` and create calculations to show actual revenua and possible growth on revenue based on the growth of online checkins in the **mart layer**:

The sql logic to build the mart: [`fct__checkins_growth`](https://github.com/Daniel-hub-es/mews_interview_2025/blob/main/mews_project/models/marts/fct/fct__checkins_growth.sql)

```sql
select *
from fct__checkins_growth;
```

The following table shows the difference between the average online revenue and the average offline revenue. 

Based on the assumption of what will happen if overall usage of online checkin is doubled, we have taken a number of guests equal to the current online checkins (N=148) and  move them from offline to online. The growth is calculated as `(148) guests x avg difference (117.826)` and sum it up to the actual total revenue, obtaining the `projected_total_revenue` thas is an increase of `1.41%` of `revenue_growth`.

| global_total_bookings | online_bookings | avg_online_rev       | avg_offline_rev      | avg_revenue_diff     | current_total_revenue | online_projected_growth_revenue | percentage_revenue_growth | projected_total_revenue  |
|-----------------------|-----------------|----------------------|----------------------|----------------------|-----------------------|---------------------------------|---------------------------|--------------------------|
| 2501                  | 148             | 605.2165221720945946 | 487.3902162080917977 | 117.8263059640027969 | 1236401.22401911      | 17438.2932826724139412          | 1.41                      | 1253839.5173017824139412 |
