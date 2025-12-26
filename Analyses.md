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

| created_year_reservation | month  | popular_rate    | gender    | popular_rate_reservations | percentage_pr | total_reservations | percentage_tr |
|--------------------------|--------|-----------------|-----------|---------------------------|---------------|--------------------|---------------|
| 2019                     | August | Fully Flexible  | female    | 78                        | 16.08         | 360                | 3.12          |
| 2019                     | August | Fully Flexible  | male      | 280                       | 57.73         | 1295               | 11.20         |
| 2019                     | August | Fully Flexible  | undefined | 127                       | 26.19         | 846                | 5.08          |
| 2019                     | August | Early - 21 days | 100       | 2                         | 0.41          | 16                 | 0.08          |



#### Popular booking rates by age_group:

| created_year_reservation | month     | popular_rate    | age_group | popular_rate_reservations | percentage_pr | total_reservations | percentage_tr |
|--------------------------|-----------|-----------------|-----------|---------------------------|---------------|--------------------|---------------|
| 2019                     | August    | Fully Flexible  | 0         | 261                       | 52.94         | 1520               | 10.44         |
| 2019                     | August    | Fully Flexible  | 25        | 56                        | 11.36         | 234                | 2.24          |
| 2019                     | August    | Fully Flexible  | 35        | 76                        | 15.42         | 279                | 3.04          |
| 2019                     | August    | Fully Flexible  | 45        | 43                        | 8.72          | 241                | 1.72          |
| 2019                     | August    | Fully Flexible  | 55        | 41                        | 8.32          | 146                | 1.64          |
| 2019                     | September | Fully Flexible  | 65        | 14                        | 2.84          | 65                 | 0.56          |
| 2019                     | August    | Early - 21 days | 100       | 2                         | 0.41          | 16                 | 0.08          |


#### Popular booking rates by nationality:

| created_year_reservation | month     | popular_rate          | nationality_code | popular_rate_reservations | percentage_pr | total_reservations | percentage_tr |
|--------------------------|-----------|-----------------------|------------------|---------------------------|---------------|--------------------|---------------|
| 2019                     | August    | Fully Flexible        | Unknown          | 131                       | 23.82         | 1096               | 5.24          |
| 2019                     | August    | Fully Flexible        | DE               | 82                        | 14.91         | 154                | 3.28          |
| 2019                     | August    | Fully Flexible        | US               | 51                        | 9.27          | 243                | 2.04          |
| 2019                     | September | Fully Flexible        | GB               | 39                        | 7.09          | 187                | 1.56          |
| 2019                     | August    | Fully Flexible        | CZ               | 30                        | 5.45          | 67                 | 1.20          |
| 2019                     | September | Fully Flexible        | SK               | 14                        | 2.55          | 72                 | 0.56          |
| 2019                     | August    | Fully Flexible        | IT               | 12                        | 2.18          | 48                 | 0.48          |
| 2019                     | August    | Fully Flexible        | RU               | 11                        | 2.00          | 51                 | 0.44          |
| 2019                     | August    | Fully Flexible        | CN               | 10                        | 1.82          | 59                 | 0.40          |
| 2019                     | August    | Fully Flexible        | FR               | 10                        | 1.82          | 46                 | 0.40          |
| 2019                     | September | Fully Flexible        | NL               | 9                         | 1.64          | 23                 | 0.36          |
| 2019                     | September | Fully Flexible        | IL               | 8                         | 1.45          | 39                 | 0.32          |
| 2019                     | August    | Fully Flexible        | CH               | 7                         | 1.27          | 18                 | 0.28          |
| 2019                     | August    | Fully Flexible        | SA               | 7                         | 1.27          | 15                 | 0.28          |
| 2019                     | August    | Fully Flexible        | AT               | 6                         | 1.09          | 17                 | 0.24          |
| 2019                     | September | Fully Flexible        | MX               | 5                         | 0.91          | 18                 | 0.20          |
| 2019                     | August    | Fully Flexible        | AU               | 5                         | 0.91          | 24                 | 0.20          |
| 2019                     | September | Non Refundable BAR BB | SE               | 5                         | 0.91          | 25                 | 0.20          |
| 2019                     | August    | Min 4 nights          | QA               | 4                         | 0.73          | 6                  | 0.16          |
| 2019                     | August    | Fully Flexible        | IE               | 4                         | 0.73          | 10                 | 0.16          |
| 2019                     | August    | Fully Flexible        | JP               | 4                         | 0.73          | 11                 | 0.16          |
| 2019                     | August    | Early - 60 days       | TH               | 4                         | 0.73          | 16                 | 0.16          |
| 2019                     | August    | Fully Flexible        | KR               | 4                         | 0.73          | 15                 | 0.16          |
| 2019                     | September | Fully Flexible        | DK               | 4                         | 0.73          | 13                 | 0.16          |
| 2019                     | September | Fully Flexible        | BE               | 4                         | 0.73          | 13                 | 0.16          |
| 2019                     | September | Fully Flexible        | BR               | 4                         | 0.73          | 11                 | 0.16          |
| 2019                     | August    | Fully Flexible        | CA               | 4                         | 0.73          | 27                 | 0.16          |
| 2019                     | September | Early - 21 days       | NO               | 4                         | 0.73          | 17                 | 0.16          |
| 2019                     | September | Fully Flexible        | LU               | 3                         | 0.55          | 6                  | 0.12          |
| 2019                     | August    | Fully Flexible        | ES               | 3                         | 0.55          | 18                 | 0.12          |
| 2019                     | September | Non Refundable BAR BB | PL               | 3                         | 0.55          | 15                 | 0.12          |
| 2019                     | August    | Fully Flexible        | HK               | 3                         | 0.55          | 5                  | 0.12          |
| 2019                     | September | Fully Flexible        | ID               | 3                         | 0.55          | 7                  | 0.12          |
| 2019                     | September | Non Refundable BAR BB | IN               | 3                         | 0.55          | 10                 | 0.12          |
| 2019                     | September | Non Refundable BAR BB | AE               | 2                         | 0.36          | 9                  | 0.08          |
| 2019                     | September | Fully Flexible        | AR               | 2                         | 0.36          | 2                  | 0.08          |
| 2019                     | August    | Fully Flexible        | BY               | 2                         | 0.36          | 4                  | 0.08          |
| 2019                     | August    | Fully Flexible        | DO               | 2                         | 0.36          | 2                  | 0.08          |
| 2019                     | September | Fully Flexible        | KW               | 2                         | 0.36          | 8                  | 0.08          |
| 2019                     | August    | Fully Flexible        | LB               | 2                         | 0.36          | 3                  | 0.08          |
| 2019                     | September | Fully Flexible        | NZ               | 2                         | 0.36          | 4                  | 0.08          |
| 2019                     | September | Early - 21 days       | PH               | 2                         | 0.36          | 3                  | 0.08          |
| 2019                     | August    | Fully Flexible        | PR               | 2                         | 0.36          | 4                  | 0.08          |
| 2019                     | September | Fully Flexible        | RO               | 2                         | 0.36          | 3                  | 0.08          |
| 2019                     | October   | Early - 60 days       | SG               | 2                         | 0.36          | 10                 | 0.08          |
| 2019                     | August    | Fully Flexible        | TR               | 2                         | 0.36          | 4                  | 0.08          |
| 2019                     | August    | Fully Flexible        | TW               | 2                         | 0.36          | 7                  | 0.08          |
| 2019                     | September | Fully Flexible        | IQ               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | September | Early - 60 days       | HU               | 1                         | 0.18          | 3                  | 0.04          |
| 2019                     | August    | Non Refundable BAR BB | BG               | 1                         | 0.18          | 2                  | 0.04          |
| 2019                     | September | Fully Flexible        | PE               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | September | Fully Flexible        | TJ               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | October   | Min 3 nights          | GR               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | August    | Fully Flexible        | AS               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | September | Fully Flexible        | PT               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | September | Early - 21 days       | FI               | 1                         | 0.18          | 3                  | 0.04          |
| 2019                     | October   | Min 3 nights          | AL               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | August    | Fully Flexible        | EE               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | August    | Min 4 nights          | CR               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | August    | Fully Flexible        | SC               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | August    | Fully Flexible        | CL               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | October   | Fully Flexible        | UA               | 1                         | 0.18          | 3                  | 0.04          |
| 2019                     | August    | Fully Flexible        | JO               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | October   | Fully Flexible        | BH               | 1                         | 0.18          | 2                  | 0.04          |
| 2019                     | September | Min 3 nights          | SY               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | September | Non Refundable BAR BB | LT               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | August    | Early - 21 days       | IS               | 1                         | 0.18          | 2                  | 0.04          |
| 2019                     | August    | Fully Flexible        | MC               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | September | Fully Flexible        | MT               | 1                         | 0.18          | 2                  | 0.04          |
| 2019                     | October   | Min 3 nights          | IR               | 1                         | 0.18          | 1                  | 0.04          |
| 2019                     | October   | Min 3 nights          | MY               | 1                         | 0.18          | 3                  | 0.04          |



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

| age_group | gender | nationality_code | rate_name             | night_count | night_cost_sum | occupied_space_sum | guest_count_sum | rev_per_capacity |
|-----------|--------|------------------|-----------------------|-------------|----------------|--------------------|-----------------|------------------|
| 0         | male   | RU               | Fully Flexible        | 1           | 682.112        | 2                  | 2               | 341.056          |
| 35        | female | GB               | Non Refundable BAR BB | 2           | 2.806          | 4                  | 2               | 0.701            |


#### Typical guesst that are most and least profitable by age:

| age_group | rate_name    | avg_revenue_per_capacity |
|-----------|--------------|--------------------------|
| 55        | Suite Offer  | 138.443                  |
| 65        | Min 5 nights | 8.859                    |

#### Typical guesst that are most and least profitable by gender:

| gender    | rate_name    | avg_revenue_per_capacity |
|-----------|--------------|--------------------------|
| male      | Suite Offer  | 156.247                  |
| undefined | Min 3 nights | 13.083                   |



#### Typical guesst that are most and least profitable by nationality:

| coalesce | rate_name       | avg_revenue_per_capacity |
|----------|-----------------|--------------------------|
| DK       | Early - 21 days | 246.473                  |
| AS       | Fully Flexible  | 0.000                    |



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

| global_total_bookings | online_bookings | avg_online_rev | avg_offline_rev | avg_revenue_diff | current_total_revenue | online_projected_growth_revenue | percentage_revenue_growth | projected_total_revenue |
|-----------------------|-----------------|----------------|-----------------|------------------|-----------------------|---------------------------------|---------------------------|-------------------------|
| 2501                  | 148             | 605.217        | 487.390         | 117.826          | 1236401.224           | 17438.293                       | 1.41                      | 1253839.517             |

