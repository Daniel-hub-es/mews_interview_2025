## 📊 Analysis

The following analysis was performed to answer the business key questions addressed for the interview as a Data Analyst for Mews Company. An investigation was performed into patterns, digital adoptions and profitability segments from the `.csv` files (`rates` and `reservations`) given by the company. This analysis leverages dbt for data modeling, SQL for behavioral analytics, and Power BI for charts and storytelling.

The content will be divided by the questions adressed.

---

### 1) What are the popular choices of booking rates (table `rate`, columns `ShortRateName` or `RateName`) for different segments of customers (table `reservation`, columns `AgeGroup`, `Gender`, `NationalityCode`)?

For this analysis, we have created a fct table called `fct__rate_popularity` from the intermediate model `int__reservations_rates` where we have joined the tables `rate` and `reservation` as a denormalization process to avoid multiple joins in the **mart** layer.

The sql logic to build the mart: [`fct__rate_popularity`](https://github.com/Daniel-hub-es/mews_interview_2025/blob/main/mews_project/models/marts/fct/fct__rate_popularity.sql)

```sql
select *
from fct__rate_popularity
limit 20;
```

The following [ad-hoc sql queries](./mews_project/analyses/key_question_one) shows the popular **booking rates** for the diferent dimensions (`age_group`, `gender` and `nationality_code`). 

#### Popular booking rates by `gender`:

| Popular Rate    | Gender    | Reservations | Percentage of Popular Rate Reservations | Total Reservations | Percentage of Popular Rate per Total Reservations |
|-----------------|-----------|--------------|-----------------------------------------|--------------------|---------------------------------------------------|
| Fully Flexible  | female    | 87           | 21.64%                                  | 360                | 3.48%                                             |
| Fully Flexible  | male      | 239          | 59.45%                                  | 1295               | 9.56%                                             |
| Early - 60 days | undefined | 76           | 18.91%                                  | 846                | 3.04%                                             |

In the stacked bar chart below is represented the distribution of reservations according to popular rate type by guest gender:

<img width="1079" height="629" alt="image" src="https://github.com/user-attachments/assets/dae52e0f-a284-4157-bbbe-5cac7ceeff9e" />

Findings shows a clear segmentation in the product choice bewteen **Fully Flexible** and **Early - 60 days**.

- The ‘Fully Flexible’ rate (in light blue) is the primary driver for both male and female segments.

  **Males** represent the largest user base for this rate, significantly outperforming other categories with over 200 reservations.

  **Females** also show a high preference for flexibility, though their total volume is lower than the male segment, totaling  87 reservations.

  Even though the “Fully Flexible” rate accounts for 81,09% of the most popular reservations by Gender, it only accounts for 13.04% of total reservations. 

- The **'undefined'** gender category are guests who prefer to engage in advanced planning and long-term booking security rather than short-term flexibility

  While the male and female segments prioritize immediate flexibility, the 'undefined' segment represents a specific niche that drives the hotel’s long-term advanced-booking pipeline


#### Popular booking rates by `age_group`:

| Popular Rate    | Age Group | Reservations | Percentage of Popular Rate Reservations | Total Reservations | Percentage of Popular Rate per Total Reservations |
|-----------------|-----------|--------------|-----------------------------------------|--------------------|---------------------------------------------------|
| Fully Flexible  | 0         | 124          | 36.15%                                  | 1520               | 4.96%                                             |
| Fully Flexible  | 25        | 53           | 15.45%                                  | 234                | 2.12%                                             |
| Fully Flexible  | 35        | 56           | 16.33%                                  | 279                | 2.24%                                             |
| Fully Flexible  | 45        | 50           | 14.58%                                  | 241                | 2.00%                                             |
| Fully Flexible  | 55        | 43           | 12.54%                                  | 146                | 1.72%                                             |
| Fully Flexible  | 65        | 14           | 4.08%                                   | 65                 | 0.56%                                             |
| Early - 21 days | 100       | 3            | 0.87%                                   | 16                 | 0.12%                                             |

In the scatter-plot below is represented the distribution of reservations according to popular rate type and guest age:

<img width="1033" height="607" alt="image" src="https://github.com/user-attachments/assets/d7c85770-2ac9-4b03-946d-e32e7e0164c2" />

Findings shows a clear segmentation in the product choice bewteen **Fully Flexible** and **Early - 21 days**.

- The ‘Fully Flexible’ rate (in light blue) is the main volume driver, especially concentrated in the Age 0 or **Unkwnown age group**, suggesting that guests who do not specify age or travel in groups prioritize cancellation security.
  
  **The range between 25 and 55 age groups** are concentrated near to the center of the graph, generating an standarized distribution of customers for a constant cashflow, being the "core business" for the Hotel.

  Although the “Fully Flexible” rate accounts for 99.13% of the most popular reservations by age group, it only accounts for 13.6% of total reservations by Age Group. 

- The ‘Early - 21 days’ rate appears as an isolated point in the upper left corner **(Age more than 65)**. This indicates that advance planning is a residual behavior or linked to a very specific age segment in this data sample.

#### Popular booking rates by `nationality` (limited to 10 records):

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

In the stacked bar chart below is represented the distribution of reservations according to popular rate type by guest nationality:

<img width="1023" height="595" alt="image" src="https://github.com/user-attachments/assets/3a0a6459-1347-4350-9b0a-6b9ea87d2f90" />



### 2) What are the typical guests who do online check-in? Is it somehow different when you compare reservations created across different weekdays (table `reservation`, `IsOnlineCheckin` column)?

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

| Dimension Type | Value | Total Reservations | Online Checkins | Percentage of Online Checkins | Percentage of Online Checkins per Total Reservations |
|----------------|-------|--------------------|-----------------|-------------------------------|------------------------------------------------------|
| Age Group      | 35    | 279                | 47              | 24.61%                        | 2.59%                                                |
| Gender         | male  | 1295               | 119             | 62.30%                        | 6.55%                                                |
| Nationality    | US    | 243                | 25              | 13.09%                        | 1.38%                                                |

#### Amount of reservations and online checkins by `gender` per weekday:

| Day of Week | Dimension Type | Value | Total Reservations | Online Checkins | Percentage of Online Checkins | Percentage of Online Checkins per Total Reservations |
|-------------|----------------|-------|--------------------|-----------------|-------------------------------|------------------------------------------------------|
| Tuesday     | Gender         | male  | 209                | 21              | 17.65%                        | 1.62%                                                |
| Wednesday   | Gender         | male  | 219                | 20              | 16.81%                        | 1.54%                                                |
| Monday      | Gender         | male  | 239                | 19              | 15.97%                        | 1.47%                                                |
| Thursday    | Gender         | male  | 228                | 18              | 15.13%                        | 1.39%                                                |
| Friday      | Gender         | male  | 163                | 15              | 12.61%                        | 1.16%                                                |
| Saturday    | Gender         | male  | 122                | 14              | 11.76%                        | 1.08%                                                |
| Sunday      | Gender         | male  | 115                | 12              | 10.08%                        | 0.93%                                                

#### Amount of reservations and online checkins by `age_group` per weekday:

| Day of Week | Dimension Type | Value | Total Reservations | Online Checkins | Percentage of Online Checkins | Percentage of Online Checkins per Total Reservations |
|-------------|----------------|-------|--------------------|-----------------|-------------------------------|------------------------------------------------------|
| Monday      | Age Group      | 35    | 51                 | 11              | 21.57%                        | 4.15%                                                |
| Thursday    | Age Group      | 35    | 49                 | 9               | 17.65%                        | 3.40%                                                |
| Tuesday     | Age Group      | 35    | 53                 | 9               | 17.65%                        | 3.40%                                                |
| Wednesday   | Age Group      | 35    | 39                 | 6               | 11.76%                        | 2.26%                                                |
| Saturday    | Age Group      | 25    | 24                 | 6               | 11.76%                        | 2.26%                                                |
| Friday      | Age Group      | 45    | 35                 | 6               | 11.76%                        | 2.26%                                                |
| Sunday      | Age Group      | 55    | 14                 | 4               | 7.84%                         | 1.51%                                                |

#### Amount of reservations and online checkins by `nationality` per weekday:

| Day of Week | Dimension Type | Value | Total Reservations | Online Checkins | Percentage of Online Checkins | Percentage of Online Checkins per Total Reservations |
|-------------|----------------|-------|--------------------|-----------------|-------------------------------|------------------------------------------------------|
| Monday      | Nationality    | GB    | 42                 | 6               | 20.69%                        | 3.17%                                                |
| Wednesday   | Nationality    | US    | 33                 | 5               | 17.24%                        | 2.65%                                                |
| Saturday    | Nationality    | US    | 29                 | 5               | 17.24%                        | 2.65%                                                |
| Tuesday     | Nationality    | US    | 34                 | 5               | 17.24%                        | 2.65%                                                |
| Friday      | Nationality    | RU    | 9                  | 3               | 10.34%                        | 1.59%                                                |
| Thursday    | Nationality    | DE    | 28                 | 3               | 10.34%                        | 1.59%                                                |
| Sunday      | Nationality    | GB    | 14                 | 2               | 6.90%                         | 1.06%                                                |

---

### 3) Look at the average night revenue per single occupied capacity. What guest segment is the most profitable per occupied space unit? And what guest segment is the least profitable?

For this analysis, we have created a fct table called `fct__revenue` from the intermediate model `int__reservations_rates` to know wich is the most and least profitable guest segment pero occupied space unit in **mart layer**:

The sql logic to build the mart: [`fct__revenue`](https://github.com/Daniel-hub-es/mews_interview_2025/blob/main/mews_project/models/marts/fct/fct__revenue.sql)

```sql
select *
from fct__revenue
limit 20;
```

The following [ad-hoc sql queries](./mews_project/analyses/key_question_three) shows the most and least profitable general guest and most and least profitable guest by dimensions (`age_group`, `gender` and `nationality_code`). 

#### Typical guesst that are most and least profitable:

| Gender    | Age Group | Nationality Code | Total Reservations | Average Daily Rate | Average Length of Stay | Average Occupancy per Booking | Revenue per Space Unit | Profitability |
|-----------|-----------|------------------|--------------------|--------------------|------------------------|-------------------------------|------------------------|---------------|
| undefined | 0         | DE               | 55                 | 221.820            | 2.1                    | 2.9                           | 6377.323               | MOST          |
| male      | 100       | SK               | 3                  | 26.679             | 2.3                    | 2.3                           | 13.339                 | LEAST         |

#### Typical guesst that are most and least profitable by `gender`:

| Gender    | Total Reservations | Average Daily Rate | Average Length of Stay | Average Occupancy per Booking | Revenue per Space Unit | Profitability |
|-----------|--------------------|--------------------|------------------------|-------------------------------|------------------------|---------------|
| undefined | 846                | 165.051            | 2.7                    | 4.3                           | 155.743                | MOST          |
| female    | 360                | 192.239            | 2.4                    | 4.2                           | 90.622                 | LEAST         |

#### Typical guesst that are most and least profitable by `age`:

| Age Group | Total Reservations | Average Daily Rate | Average Length of Stay | Average Occupancy per Booking | Revenue per Space Unit | Profitability |
|-----------|--------------------|--------------------|------------------------|-------------------------------|------------------------|---------------|
| 0         | 1520               | 183.704            | 2.8                    | 4.8                           | 144.872                | MOST          |
| 65        | 65                 | 126.777            | 3.2                    | 4.4                           | 59.445                 | LEAST         |

#### Typical guesst that are most and least profitable by `nationality`:

| Nationality Code | Total Reservations | Average Daily Rate | Average Length of Stay | Average Occupancy per Booking | Revenue per Space Unit | Profitability |
|------------------|--------------------|--------------------|------------------------|-------------------------------|------------------------|---------------|
| ID               | 7                  | 211.736            | 2.0                    | 3.1                           | 741.076                | MOST          |
| AL               | 1                  | 73.227             | 3.0                    | 6.0                           | 36.613                 | LEAST         |

---

### 4) Bonus: As a bonus assignment, we want to motivate our hotels to promote online checkin and we want to give them some hard data. Look at the data and your analysis again and estimate what would be the impact on total room revenue if the overall usage of online checkin doubled.

For this analysis, we have created a fct table called `fct__checkins_growth` from the intermediate model `int__reservations_rates` and create calculations to show actual revenua and possible growth on revenue based on the growth of online checkins in the **mart layer**:

The sql logic to build the mart: [`fct__checkins_growth`](https://github.com/Daniel-hub-es/mews_interview_2025/blob/main/mews_project/models/marts/fct/fct__checkins_growth.sql)

```sql
select *
from fct__checkins_growth;
```

The following table shows the difference between the average online revenue and the average offline revenue. 

Based on the assumption of what will happen if overall usage of online checkin is doubled, we have taken a number of guests equal to the current online checkins (N=148) and  move them from offline to online. The growth is calculated as `(148) guests x avg difference (117.826)` and sum it up to the actual total revenue, obtaining the `projected_total_revenue` thas is an increase of `1.41%` of `revenue_growth`.

| Total Bookings | Online Bookings | Avg Online Total Stay | Avg Offline Total Stay | Revenue Lift per Stay | Online Average Daily Rate | Offline Average Daily Rate | Average Daily Rate Difference | Current Total Revenue | Projected Revenue Lift | Growth % | Projected Total Revenue |
|----------------|-----------------|-----------------------|------------------------|-----------------------|---------------------------|----------------------------|-------------------------------|-----------------------|------------------------|----------|-------------------------|
| 2501           | 148             | 605.22                | 487.39                 | 117.83                | 230.86                    | 183.85                     | 47.01                         | 1236401.22            | 17438.29               | 1.41%    | 1253839.52              |
