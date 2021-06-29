
--1) Daily New Users
WITH game_d AS (SELECT user_id, to_date(event_timestamp) as event FROM game)
select count(distinct(user_id)) as new_users, event from (
	select user_id, event, dense_rank() OVER(PARTITION BY user_id ORDER BY event) as d from game_d
	) as a 
	where d = 1 and (current_date() - interval '14' day) < event
	group by event;

--2) Daily Returning Users
WITH game_d AS (SELECT user_id, to_date(event_timestamp) as event FROM game)
select count(distinct(user_id)) as returning_users, event from (
	select user_id, event, dense_rank() OVER(PARTITION BY user_id ORDER BY event) as d from game_d
	) as a 
	where d > 1 and (current_date() - interval '14' day) < event
	group by event;


--3) Weekly Active Users
 select count(distinct(user_id)) as active_users, weekofyear(event_timestamp) from game 
 	where year(event_timestamp) = 2021
 	group by weekofyear(event_timestamp);
 
--4) Average Revenue Per Paying User 
select round(avg(amount),2) as ARPPU, month(event_timestamp) from game 
	where year(event_timestamp) = 2021 and amount is not null
	group by month(event_timestamp); 

--5) 3rd, 7th Day Retention
WITH game_d AS (SELECT user_id, to_date(event_timestamp) as event FROM game)
select 	t.report_day as ddate,
		round(sum(t.day3)/count(t.usr)*100, 2) as 3rd_day_ret,
		round(sum(t.day7)/count(t.usr)*100,2) as 7th_day_ret
from (
	select 	a.usr_id as usr,
			a.my_first_day,
			b.event as report_day,
			case when datediff(b.event, a.my_first_day) = 2 then 1 else 0 end day3,
			case when datediff(b.event, a.my_first_day) = 6 then 1 else 0 end day7
			from (
				select 	distinct(user_id) as usr_id,
						min(event) over (partition by user_id) as my_first_day from game_d
				) a right join (
	select distinct(user_id), event from game_d where (current_date() - interval '14' day) < event) b on a.usr_id = b.user_id
	) t
group by report_day;

