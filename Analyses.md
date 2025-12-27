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

| Popular Rate    | Gender    | Reservations | Percentage of Popular Rate Reservations | Total Reservations | Percentage of Popular Rate per Total Reservations |
|-----------------|-----------|--------------|-----------------------------------------|--------------------|---------------------------------------------------|
| Fully Flexible  | female    | 87           | 21.64%                                  | 360                | 3.48%                                             |
| Fully Flexible  | male      | 239          | 59.45%                                  | 1295               | 9.56%                                             |
| Early - 60 days | undefined | 76           | 18.91%                                  | 846                | 3.04%                                             |

#### Popular booking rates by age_group:

| Popular Rate    | Age Group | Reservations | Percentage of Popular Rate Reservations | Total Reservations | Percentage of Popular Rate per Total Reservations |
|-----------------|-----------|--------------|-----------------------------------------|--------------------|---------------------------------------------------|
| Fully Flexible  | 0         | 124          | 36.15%                                  | 1520               | 4.96%                                             |
| Fully Flexible  | 25        | 53           | 15.45%                                  | 234                | 2.12%                                             |
| Fully Flexible  | 35        | 56           | 16.33%                                  | 279                | 2.24%                                             |
| Fully Flexible  | 45        | 50           | 14.58%                                  | 241                | 2.00%                                             |
| Fully Flexible  | 55        | 43           | 12.54%                                  | 146                | 1.72%                                             |
| Fully Flexible  | 65        | 14           | 4.08%                                   | 65                 | 0.56%                                             |
| Early - 21 days | 100       | 3            | 0.87%                                   | 16                 | 0.12%                                             |

#### Popular booking rates by nationality (limited to 10 records):

| Popular Rate    | Nationality | Reservations | Percentage of Popular Rate Reservations | Total Reservations | Percentage of Popular Rate per Total Reservations |
|-----------------|-------------|--------------|-----------------------------------------|--------------------|---------------------------------------------------|
| Early - 60 days | Unknown     | 76           | 16.63%                                  | 1096               | 3.04%                                             |
| Fully Flexible  | GB          | 40           | 8.75%                                   | 187                | 1.60%                                             |
| Fully Flexible  | DE          | 26           | 5.69%                                   | 154                | 1.04%                                             |
| Fully Flexible  | US          | 26           | 5.69%                                   | 243                | 1.04%                                             |
| Fully Flexible  | IT          | 15           | 3.28%                                   | 48                 | 0.60%                                             |
| Fully Flexible  | FR          | 14           | 3.06%                                   | 46                 | 0.56%                                             |
| Fully Flexible  | CZ          | 14           | 3.06%                                   | 67                 | 0.56%                                             |
| Fully Flexible  | SK          | 14           | 3.06%                                   | 72                 | 0.56%                                             |
| Fully Flexible  | RU          | 12           | 2.63%                                   | 51                 | 0.48%                                             |
| Fully Flexible  | CN          | 11           | 2.41%                                   | 59                 | 0.44%                                             |

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

| Age Group | Gender | Nationality | Online checkins |
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

