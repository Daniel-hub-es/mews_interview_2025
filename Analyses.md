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

Findings shows a clear segmentation in the product choice bewteen **‘Fully Flexible’** and **Early - 60 days**.

- The ‘Fully Flexible’ rate (in light blue) is the primary driver for both male and female segments.

  **Males** represent the largest user base for this rate, significantly outperforming other categories with over 200 reservations.

  **Females** also show a high preference for flexibility, though their total volume is lower than the male segment, totaling  87 reservations.

  Even though the ‘Fully Flexible’ rate accounts for 81,09% of the most popular reservations by Gender, it only accounts for 13.04% of total reservations. 

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

Findings shows a clear segmentation in the product choice bewteen **‘Fully Flexible’** and **Early - 21 days**.

- The ‘Fully Flexible’ rate (in light blue) is the main volume driver, especially concentrated in the Age 0 or **Unkwnown age group**, suggesting that guests who do not specify age or travel in groups prioritize cancellation security.
  
  **The range between 25 and 55 age groups** are concentrated near to the center of the graph, generating an standarized distribution of customers for a constant cashflow, being the "core business" for the Hotel.

  Although the ‘Fully Flexible’ rate accounts for 99.13% of the most popular reservations by age group, it only accounts for 13.6% of total reservations by Age Group. 

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

- **The "Unknown" Data Gap**:

  This category represents the single highest volume with  76 reservations and preference for ‘Early - 60 days’. The high prevalence of "Unknown" suggests a significant portion of guests whose profile data is missing or didn't wanted to give this type of data, a common insight to improve check-in data or to identify specific high-volume booking channels.

- **Primary Identified Markets**:

  - **Great Britain (GB)** prefers ‘Fully Flexible’ Rate reservation and is the leading identified nationality with 40 reservations in the table shown above, establishing it as the most important market.
    
  - **Germany (DE)** and the **United States (US)** follow as the next major tiers, both contributing 26 reservations each prefering ‘Fully Flexible’ Rate.

Across the three dimensions we can assume that the preference for Booking Rates categories is **‘Fully Flexible’**. The data suggests that guests prioritize cancellation security and travel agility over the discounts of the "Early" rates.

---

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

The table above shows the profile crossing the dimensions of `Age Group`, `Gender` and `Nationality` that makes more online checkins compared to the rest of the guests of the datased given. 

It shows a male in the middle age from Great Britain. The concentration arround this segment suggest that the typical user is an early adpoter of online checkin technologies as first solution for comodity compared to the rest of mid users that we have data available. 

Information suggest that a male arround age between 25 and 35 would be considered as a "Milennial" or "Gen Z", generations of digital natives who will consider the use of technology in their behavior when checking in online at the establishment.

This profile can be used as the target for the marketing strategies where the Hotel need to invest if they want to promote the usage of online checkins as it reports more protif in average compared to the offline checkin. 

#### Amount of reservations and online checkins by dimension:

| Dimension Type | Value | Total Reservations | Online Checkins | Percentage of Online Checkins | Percentage of Online Checkins per Total Reservations |
|----------------|-------|--------------------|-----------------|-------------------------------|------------------------------------------------------|
| Age Group      | 35    | 279                | 47              | 24.61%                        | 2.59%                                                |
| Gender         | male  | 1295               | 119             | 62.30%                        | 6.55%                                                |
| Nationality    | US    | 243                | 25              | 13.09%                        | 1.38%                                                |

If we consider the dimensions separately, the result is similar:

- The predominant age group continue to be the segment of Age between 25 and 35.
  
- The predomintant gender continue to be a male
  
- The nationality is the US. If we compare it with the table presented above, we see that the language and part of the culture are common to both.

#### Amount of reservations and online checkins by `gender` per weekday:

| Day of Week | Dimension Type | Value | Total Reservations | Online Checkins | Percentage of Online Checkins | Percentage of Online Checkins per Total Reservations |
|-------------|----------------|-------|--------------------|-----------------|-------------------------------|------------------------------------------------------|
| Tuesday     | Gender         | male  | 209                | 21              | 17.65%                        | 1.62%                                                |
| Wednesday   | Gender         | male  | 219                | 20              | 16.81%                        | 1.54%                                                |
| Monday      | Gender         | male  | 239                | 19              | 15.97%                        | 1.47%                                                |
| Thursday    | Gender         | male  | 228                | 18              | 15.13%                        | 1.39%                                                |
| Friday      | Gender         | male  | 163                | 15              | 12.61%                        | 1.16%                                                |
| Saturday    | Gender         | male  | 122                | 14              | 11.76%                        | 1.08%                                                |
| Sunday      | Gender         | male  | 115                | 12              | 10.08%                        | 0.93%                                                ç

In the stacked bar chart below is represented the distribution of the guest who do more online checkins online checkins per weekday by the gender of the guest:

<img width="763" height="452" alt="image" src="https://github.com/user-attachments/assets/7bbec2af-82cf-40ce-8fc7-2600aed1482f" />

As mentioned previously, if we split online reservations by weekdays by gender to compare, the result shows that the male is the gender that do more online checkins every weekday. 

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

In the stacked bar chart below is represented the distribution of the guest who do more online checkins  per weekday by the Age Group of the guest:

<img width="738" height="434" alt="image" src="https://github.com/user-attachments/assets/92ceb15a-133a-43bc-b766-a4e866cf29a1" />

By age, behavior varies by day of the week:

- From Monday to Thursday, behavior is significant for digital adopters aged 25 to 35, and the number of online reservations is remarkably consistent.
  
- Fridays the Ageage group rises from 35 to 45.

- On Saturdays, the digital leadership shifts to a younger demographic from 0 to 25, although with a lower volume.

- On Sundays, the preference shifts to an older demographic from 45 to 55, which shows the lowest relative volume

- The "Percentage of Online Checkins per Total Reservations" column shows that the Monday/Age 35 segment is the single most important contributor to the hotel's total digital pipeline.

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

In the stacked bar chart below is represented the distribution of the guest who do more online checkins per weekday by the nationality of the guest:

<img width="738" height="449" alt="image" src="https://github.com/user-attachments/assets/fd523b78-b9d3-439d-b645-4dbf2137e26a" />

- Just like in the age analysis, Monday is the peak day for digital adoption. Guests from Great Britain (GB) lead this trend as it was seen in analyses mentioned before.

- This reinforces that the "Typical Guest" (British, Age from 25 to 35, Male) is most active at the beginning of the week.

- The US market shows remarkable consistency in digital behavior, maintaining a steady volume of online checkins.

- unday remains the day with the lowest digital engagement, This shows that weekend bookings tend to be less than beggining of the week online bookings.

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

The following metrics were calculated to identify the most and leat profitable guest:

- Average Daily Rate (ADR): average price that a guest pay per night reservation

$$\frac{\sum(\text{night cost sum})}{\sum(\text{night count})}$$

- Average Lenght of Stay: average nights that a guest stay per reservation

$$\frac{\sum(\text{night count})}{(\text{reservations}}$$

- Average Occupancy per Booking: average occupancy of guests per room by reservation

$$\frac{\sum(\text{guest count sum})}{(\text{reservations})}$$

- Revenue per Space Unit: income by capacity unit avialable per room

$$\frac{\sum(\text{night cost sum})}{\sum(\text{occupied space sum}))}$$

The most and leas profitable guests were ranked based on Revenue Per Space Unit. This ensures the profitabily measured by efficiency of the resources rather than only the total volume. 

The most profitable segments are those that fills the gap between high nightly rates and high occupancy density, while least profitable segments are characterized by low ADR that fail to justify the physical space allocated to the guest.


#### Typical guesst that are most and least profitable:

| Gender    | Age Group | Nationality Code | Total Reservations | Average Daily Rate | Average Length of Stay | Average Occupancy per Booking | Revenue per Space Unit | Profitability |
|-----------|-----------|------------------|--------------------|--------------------|------------------------|-------------------------------|------------------------|---------------|
| undefined | 0         | DE               | 55                 | 221.820            | 2.1                    | 2.9                           | 6377.323               | MOST          |
| male      | 100       | SK               | 3                  | 26.679             | 2.3                    | 2.3                           | 13.339                 | LEAST         |

With the data showed in the table above, we can identify generally that the most profitable guest is from Germany as que don't know the Age Group of the guest or the Gender. The ADR is the highest for this guest, maximizing space capacity while maintaining a high price ceiling.

#### Typical guesst that are most and least profitable by `gender`:

| Gender    | Total Reservations | Average Daily Rate | Average Length of Stay | Average Occupancy per Booking | Revenue per Space Unit | Profitability |
|-----------|--------------------|--------------------|------------------------|-------------------------------|------------------------|---------------|
| undefined | 846                | 165.051            | 2.7                    | 4.3                           | 155.743                | MOST          |
| female    | 360                | 192.239            | 2.4                    | 4.2                           | 90.622                 | LEAST         |

The most profitable guest by gender is the "undefined" gender. While they pay a lower ADR, they lead in Revenue per Space Unit diven by the highest Average Occupancy Per Booking and the longest Stay. Sugest groups or families where profiles weren't captured.

The most profitable guest by gender is the female gender. While this group pays higher ADR, they stay for shorter periods  and have slightly lower occupancy density.

#### Typical guesst that are most and least profitable by `age`:

| Age Group | Total Reservations | Average Daily Rate | Average Length of Stay | Average Occupancy per Booking | Revenue per Space Unit | Profitability |
|-----------|--------------------|--------------------|------------------------|-------------------------------|------------------------|---------------|
| 0         | 1520               | 183.704            | 2.8                    | 4.8                           | 144.872                | MOST          |
| 65        | 65                 | 126.777            | 3.2                    | 4.4                           | 59.445                 | LEAST         |

The "unknown" age group terpesent the primary source of business based on the amount of reservations and have the highest efficiency by Revenue per Space Space Unit. They pack rooms making square meters productive.

The low segments are adults between 55 and 65, with a lower ADR. Their occupancy is still high but lowe compared to the other group. 

#### Typical guesst that are most and least profitable by `nationality`:

| Nationality Code | Total Reservations | Average Daily Rate | Average Length of Stay | Average Occupancy per Booking | Revenue per Space Unit | Profitability |
|------------------|--------------------|--------------------|------------------------|-------------------------------|------------------------|---------------|
| ID               | 7                  | 211.736            | 2.0                    | 3.1                           | 741.076                | MOST          |
| AL               | 1                  | 73.227             | 3.0                    | 6.0                           | 36.613                 | LEAST         |

The ID (Indonesia) segment got a very high ADR even if the lenght of stay is shorter, a small lucrative niche with with a high Revenue per Space Unit. 

The AL (Albania) segment has a massive occupancy, and a long  Average Length of Stay, but it fails to be profitable, with a really low ADR. The Revenue per Space Unit is low, high occupancy cannot compensate for a weak Average Daily Rate, filling a room with 6 people at a low price is less efficient than filling it with 3 people at a premium price.

---

### 4) Bonus: As a bonus assignment, we want to motivate our hotels to promote online checkin and we want to give them some hard data. Look at the data and your analysis again and estimate what would be the impact on total room revenue if the overall usage of online checkin doubled.

For this analysis, we have created a fct table called `fct__checkins_growth` from the intermediate model `int__reservations_rates` and create calculations to show actual revenua and possible growth on revenue based on the growth of online checkins in the **mart layer**:

The sql logic to build the mart: [`fct__checkins_growth`](https://github.com/Daniel-hub-es/mews_interview_2025/blob/main/mews_project/models/marts/fct/fct__checkins_growth.sql)

```sql
select *
from fct__checkins_growth;
```

The following table shows the difference between the average online revenue and the average offline revenue. 

| Total Bookings | Online Bookings | Avg Online Total Stay | Avg Offline Total Stay | Revenue Lift per Stay | Online Average Daily Rate | Offline Average Daily Rate | Average Daily Rate Difference | Current Total Revenue | Projected Revenue Lift | Growth % | Projected Total Revenue |
|----------------|-----------------|-----------------------|------------------------|-----------------------|---------------------------|----------------------------|-------------------------------|-----------------------|------------------------|----------|-------------------------|
| 2501           | 148             | 605.22                | 487.39                 | 117.83                | 230.86                    | 183.85                     | 47.01                         | 1236401.22            | 17438.29               | 1.41%    | 1253839.52              |

The data presented gives significant performance gap between guest who do checkin online versus who do so offline.

- The Revenue Lift Per Stay shows 117.83$ more per stay than the offline guests (605,22$ vs 487,39$).

- The Online Average Daily Rate (ADR) is 47,01$ higher for the online guests than the offline guests.

The resullts suggest that the digital users are higher spending profiles.

The table calculates a financial benefit of increasing the digital adoption. If the amount of online guests is doubled, the stablishment would see an increase of 17,438.20$, a 1,41% increase of the total revenue. In rounded numbers, the business could increase the total revenue from $1,236M to $1,253M. 

---

The related Power Bi presentations is in the following path: [mews_interview_2025/resources
/pbi](https://github.com/Daniel-hub-es/mews_interview_2025/tree/main/resources/pbi)
