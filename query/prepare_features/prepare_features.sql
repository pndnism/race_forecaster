CREATE TEMP FUNCTION
  parse_goal_time(goal_time string) as 
    (cast((left(goal_time, instr(goal_time, ":" )-1)) as float64) * 60 + 
        cast(right(goal_time, length(goal_time) - instr(goal_time, ":" )) as float64));

# CREATE TEMP FUNCTION
#   parse_arrival_orders(arrival_orders string) as 
#     ();

#create or replace table horse_race_processed.race_features as 
with 
    race_info as (
        SELECT
            race_id,
            cast(horse_ids as string) as horse_id,
            arrival_orders,
            box_numbers,
            horse_numbers,
            horse_sexes,
            horse_ages,
            jockey_weights,
            jockey_names,
            goal_times,
            order_transitions,
            half_times,
            odds,
            popularities,
            horse_weights,
            horse_weight_diffs,
            trainer_names,
            horse_owners,
            race_number,
            course_type,
            course_direction,
            course_length,
            weather,
            course_condition,
            race_start_time,
            held_date,
            held_place,
            held_number,
            held_date_number
        FROM horse_race_prediction.race_stackings
    ),

    horse_info as (
        SELECT 
            horse_id,
            horse_name,
            producer,
            origin_place,
            mother,
            father,
            mother_of_father,
            father_of_father,
            mother_of_mother,
            father_of_mother
        FROM horse_race_prediction.horse_stackings
    ),
    -- レコードを中央競馬に絞る
    center_race_info as (
        select 
            * 
        from race_info
        where held_place in ("札幌","函館","福島","新潟","中山","東京","中京","京都","阪神","小倉")
        ),
    # base_categorical_features as (
    #     select 
    #         race_id,
    #         horse_id,
    #         held_place,
    #         held_date,
    #         box_numbers,
    #         horse_numbers,
    #         horse_sexes,
    #         jockey_names,
    #         trainer_names,
    #         horse_owners,
    #         race_number,
    #         course_type,
    #         course_direction,
    #         weather,
    #         course_condition,
    #         race_start_time,
    #         REGEXP_EXTRACT(held_date, r'\d+月') as held_month,
    #         EXTRACT(DAYOFWEEK from DATE(cast(REGEXP_EXTRACT(held_date, r'(\d+)年') as int64), cast(REGEXP_EXTRACT(held_date, r'(\d+)月') as int64),cast(REGEXP_EXTRACT(held_date, r'(\d+)日') as int64))) as held_day_of_week,
    #         held_place,
    #         case 
    #           when course_length < 1400 then '短距離'
    #           when course_length < 1800 then 'マイル'
    #           when course_length < 2200 then '中距離'
    #           when course_length < 2800 then '中長距離'
    #           else '長距離'
    #         end course_length_category,
    #         producer,
    #         origin_place,
    #         mother,
    #         father,
    #         mother_of_mother,
    #         father_of_mother,
    #         mother_of_father,
    #         father_of_father
    #         from center_race_info
    #         left outer join horse_info
    #         using (horse_id)
    # ),

    base_features as (
        select 
            race_id,
            horse_id,
            held_place,
            weather,
            jockey_names,
            course_type,
            course_condition,
            course_direction,
            race_number,
            REGEXP_EXTRACT(held_date, r'\d+月') as held_month,
            EXTRACT(DAYOFWEEK from DATE(cast(REGEXP_EXTRACT(held_date, r'(\d+)年') as int64), cast(REGEXP_EXTRACT(held_date, r'(\d+)月') as int64),cast(REGEXP_EXTRACT(held_date, r'(\d+)日') as int64))) as held_day_of_week,
            case 
              when course_length < 1400 then '短距離'
              when course_length < 1800 then 'マイル'
              when course_length < 2200 then '中距離'
              when course_length < 2800 then '中長距離'
              else '長距離'
            end course_length_category,            
            DATE(REGEXP_REPLACE(REPLACE(held_date, "日",""), "年|月", "-")) as held_date,
            cast(horse_ages as int64) as horse_ages,
            cast(jockey_weights as float64) as jockey_weights,
            parse_goal_time(goal_times) as goal_times,
            cast(half_times as float64) as half_times,
            cast(horse_weights as float64) as horse_weights,
            cast(horse_weight_diffs as float64) as horse_weight_diffs,
            cast(course_length as float64) as course_length,
            cast(jockey_weights as float64)/cast(horse_weights as float64) as weight_ratio,
            cast(course_length as float64)/parse_goal_time(goal_times) as avg_velocity,
            (cast(course_length as float64)/2)/cast(half_times as float64) as avg_half_velocity,
            SAFE_CAST(arrival_orders as int64) as arrival_orders
        from center_race_info
        left outer join horse_info
        using (horse_id)
    ),

    previous_1_races_avgs as (
        select
            race_id,
            horse_id,
            held_date,
            AVG(goal_times) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS previous_1_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS previous_1_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS previous_1_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS previous_1_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS previous_1_arrival_orders_avgs
        from base_features
        order by horse_id desc, held_date desc
    ),
    previous_3_races_avgs as (
        select
            race_id,
            horse_id,
            held_date,
            AVG(goal_times) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS previous_3_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS previous_3_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS previous_3_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS previous_3_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS previous_3_arrival_orders_avgs
        from base_features
        order by horse_id desc, held_date desc
    ),
    previous_5_races_avgs as (
        select
            race_id,
            horse_id,
            held_date,
            AVG(goal_times) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS previous_5_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS previous_5_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS previous_5_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS previous_5_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS previous_5_arrival_orders_avgs
        from base_features
        order by horse_id desc, held_date desc
    ),
    previous_1_same_place_races_avgs as (
        select
            race_id,
            horse_id,
            held_place,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_place_previous_1_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_place_previous_1_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_place_previous_1_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_place_previous_1_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_place_previous_1_arrival_orders_avgs,
        from base_features
        order by horse_id desc, held_place desc, held_date desc
    ),
    previous_same_place_3_races_avgs as (
        select
            race_id,
            horse_id,
            held_place,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_place_previous_3_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_place_previous_3_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_place_previous_3_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_place_previous_3_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_place_previous_3_arrival_orders_avgs,
        from base_features
        order by horse_id desc, held_place desc, held_date desc
    ),
    previous_same_place_5_races_avgs as (
        select
            race_id,
            horse_id,
            held_place,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_place_previous_5_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_place_previous_5_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_place_previous_5_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_place_previous_5_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,held_place ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_place_previous_5_arrival_orders_avgs,
        from base_features
        order by horse_id desc, held_place desc, held_date desc
    ),
    previous_1_same_weather_races_avgs as (
        select
            race_id,
            horse_id,
            weather,
            held_date,
            AVG(goal_times) OVER (partition by horse_id, weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS previous_1_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id, weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS previous_1_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id, weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS previous_1_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id, weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS previous_1_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id, weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS previous_1_arrival_orders_avgs,
        from base_features
        order by horse_id desc, weather desc, held_date desc
    ),
    previous_same_weather_3_races_avgs as (
        select
            race_id,
            horse_id,
            weather,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_weather_previous_3_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_weather_previous_3_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_weather_previous_3_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_weather_previous_3_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_weather_previous_3_arrival_orders_avgs,
        from base_features
        order by horse_id desc, weather desc, held_date desc
    ),
    previous_same_weather_5_races_avgs as (
        select
            race_id,
            horse_id,
            weather,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_weather_previous_5_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_weather_previous_5_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_weather_previous_5_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_weather_previous_5_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,weather ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_weather_previous_5_arrival_orders_avgs,
        from base_features
        order by horse_id desc, weather desc, held_date desc
    ),
    previous_1_same_jockey_races_avgs as (
        select
            race_id,
            horse_id,
            jockey_names,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_jockey_names_previous_1_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_jockey_names_previous_1_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_jockey_names_previous_1_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_jockey_names_previous_1_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_jockey_names_previous_1_arrival_orders_avgs,
        from base_features
        order by horse_id desc, jockey_names desc, held_date desc
    ),
    previous_same_jockey_3_races_avgs as (
        select
            race_id,
            horse_id,
            jockey_names,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_jockey_names_previous_3_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_jockey_names_previous_3_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_jockey_names_previous_3_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_jockey_names_previous_3_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_jockey_names_previous_3_arrival_orders_avgs,
        from base_features
        order by horse_id desc, jockey_names desc, held_date desc
    ),
    previous_same_jockey_5_races_avgs as (
        select
            race_id,
            horse_id,
            jockey_names,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_jockey_names_previous_5_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_jockey_names_previous_5_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_jockey_names_previous_5_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_jockey_names_previous_5_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,jockey_names ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_jockey_names_previous_5_arrival_orders_avgs,
        from base_features
        order by horse_id desc, jockey_names desc, held_date desc
    ),
    previous_1_same_course_type_races_avgs as (
        select
            race_id,
            horse_id,
            course_type,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_type_previous_1_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_type_previous_1_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_type_previous_1_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_type_previous_1_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_type_previous_1_arrival_orders_avgs,
        from base_features
        order by horse_id desc, course_type desc, held_date desc
    ),
    previous_3_same_course_type_races_avgs as (
        select
            race_id,
            horse_id,
            course_type,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_type_previous_3_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_type_previous_3_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_type_previous_3_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_type_previous_3_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_type_previous_3_arrival_orders_avgs,
        from base_features
        order by horse_id desc, course_type desc, held_date desc
    ),
    previous_5_same_course_type_races_avgs as (
        select
            race_id,
            horse_id,
            course_type,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_type_previous_5_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_type_previous_5_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_type_previous_5_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_type_previous_5_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,course_type ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_type_previous_5_arrival_orders_avgs,
        from base_features
        order by horse_id desc, course_type desc, held_date desc
    ),
    previous_1_same_course_condition_races_avgs as (
        select
            race_id,
            horse_id,
            course_condition,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_condition_previous_1_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_condition_previous_1_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_condition_previous_1_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_condition_previous_1_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_condition_previous_1_arrival_orders_avgs,
        from base_features
        order by horse_id desc, course_condition desc, held_date desc
    ),
    previous_3_same_course_condition_races_avgs as (
        select
            race_id,
            horse_id,
            course_condition,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_condition_previous_3_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_condition_previous_3_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_condition_previous_3_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_condition_previous_3_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_condition_previous_3_arrival_orders_avgs,
        from base_features
        order by horse_id desc, course_condition desc, held_date desc
    ),
    previous_5_same_course_condition_races_avgs as (
        select
            race_id,
            horse_id,
            course_condition,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_condition_previous_5_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_condition_previous_5_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_condition_previous_5_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_condition_previous_5_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,course_condition ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_condition_previous_5_arrival_orders_avgs,
        from base_features
        order by horse_id desc, course_condition desc, held_date desc
    ),
    previous_1_same_course_direction_races_avgs as (
        select
            race_id,
            horse_id,
            course_direction,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_direction_previous_1_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_direction_previous_1_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_direction_previous_1_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_direction_previous_1_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_direction_previous_1_arrival_orders_avgs,
        from base_features
        order by horse_id desc, course_direction desc, held_date desc
    ),
    previous_3_same_course_direction_races_avgs as (
        select
            race_id,
            horse_id,
            course_direction,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_direction_previous_3_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_direction_previous_3_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_direction_previous_3_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_direction_previous_3_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_direction_previous_3_arrival_orders_avgs,
        from base_features
        order by horse_id desc, course_direction desc, held_date desc
    ),
    previous_5_same_course_direction_races_avgs as (
        select
            race_id,
            horse_id,
            course_direction,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_direction_previous_5_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_direction_previous_5_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_direction_previous_5_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_direction_previous_5_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,course_direction ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_direction_previous_5_arrival_orders_avgs,
        from base_features
        order by horse_id desc, course_direction desc, held_date desc
    ),
    previous_1_same_race_number_races_avgs as (
        select
            race_id,
            horse_id,
            race_number,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_race_number_previous_1_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_race_number_previous_1_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_race_number_previous_1_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_race_number_previous_1_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_race_number_previous_1_arrival_orders_avgs,
        from base_features
        order by horse_id desc, race_number desc, held_date desc
    ),
    previous_3_same_race_number_races_avgs as (
        select
            race_id,
            horse_id,
            race_number,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_race_number_previous_3_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_race_number_previous_3_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_race_number_previous_3_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_race_number_previous_3_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_race_number_previous_3_arrival_orders_avgs,
        from base_features
        order by horse_id desc, race_number desc, held_date desc
    ),
    previous_5_same_race_number_races_avgs as (
        select
            race_id,
            horse_id,
            race_number,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_race_number_previous_5_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_race_number_previous_5_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_race_number_previous_5_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_race_number_previous_5_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,race_number ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_race_number_previous_5_arrival_orders_avgs,
        from base_features
        order by horse_id desc, race_number desc, held_date desc
    ),
    previous_1_same_course_length_category_races_avgs as (
        select
            race_id,
            horse_id,
            course_length_category,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_length_category_previous_1_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_length_category_previous_1_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_length_category_previous_1_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_length_category_previous_1_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_course_length_category_previous_1_arrival_orders_avgs,
        from base_features
        order by horse_id desc, course_length_category desc, held_date desc
    ),
    previous_3_same_course_length_category_races_avgs as (
        select
            race_id,
            horse_id,
            course_length_category,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_length_category_previous_3_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_length_category_previous_3_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_length_category_previous_3_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_length_category_previous_3_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_course_length_category_previous_3_arrival_orders_avgs,
        from base_features
        order by horse_id desc, course_length_category desc, held_date desc
    ),
    previous_5_same_course_length_category_races_avgs as (
        select
            race_id,
            horse_id,
            course_length_category,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_length_category_previous_5_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_length_category_previous_5_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_length_category_previous_5_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_length_category_previous_5_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,course_length_category ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_course_length_category_previous_5_arrival_orders_avgs,
        from base_features
        order by horse_id desc, course_length_category desc, held_date desc
    ),
    previous_1_same_held_month_races_avgs as (
        select
            race_id,
            horse_id,
            held_month,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_month_previous_1_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_month_previous_1_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_month_previous_1_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_month_previous_1_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_month_previous_1_arrival_orders_avgs,
        from base_features
        order by horse_id desc, held_month desc, held_date desc
    ),
    previous_3_same_held_month_races_avgs as (
        select
            race_id,
            horse_id,
            held_month,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_month_previous_3_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_month_previous_3_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_month_previous_3_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_month_previous_3_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_month_previous_3_arrival_orders_avgs,
        from base_features
        order by horse_id desc, held_month desc, held_date desc
    ),
    previous_5_same_held_month_races_avgs as (
        select
            race_id,
            horse_id,
            held_month,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_month_previous_5_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_month_previous_5_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_month_previous_5_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_month_previous_5_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,held_month ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_month_previous_5_arrival_orders_avgs,
        from base_features
        order by horse_id desc, held_month desc, held_date desc
    ),
    previous_1_same_held_day_of_week_races_avgs as (
        select
            race_id,
            horse_id,
            held_day_of_week,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_day_of_week_previous_1_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_day_of_week_previous_1_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_day_of_week_previous_1_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_day_of_week_previous_1_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS same_held_day_of_week_previous_1_arrival_orders_avgs,
        from base_features
        order by horse_id desc, held_day_of_week desc, held_date desc
    ),
    previous_3_same_held_day_of_week_races_avgs as (
        select
            race_id,
            horse_id,
            held_day_of_week,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_day_of_week_previous_3_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_day_of_week_previous_3_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_day_of_week_previous_3_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_day_of_week_previous_3_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS same_held_day_of_week_previous_3_arrival_orders_avgs,
        from base_features
        order by horse_id desc, held_day_of_week desc, held_date desc
    ),
    previous_5_same_held_day_of_week_races_avgs as (
        select
            race_id,
            horse_id,
            held_day_of_week,
            held_date,
            AVG(goal_times) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_day_of_week_previous_5_goal_times_avgs,
            AVG(half_times) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_day_of_week_previous_5_half_times_avgs,
            AVG(avg_velocity) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_day_of_week_previous_5_avg_velocity_avgs,
            AVG(avg_half_velocity) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_day_of_week_previous_5_avg_half_velocity_avgs,
            AVG(arrival_orders) OVER (partition by horse_id,held_day_of_week ORDER BY held_date ASC ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) AS same_held_day_of_week_previous_5_arrival_orders_avgs,
        from base_features
        order by horse_id desc, held_day_of_week desc, held_date desc
    )



select * from base_features 
left join previous_1_races_avgs using (race_id, horse_id, held_date)
left join previous_3_races_avgs using (race_id, horse_id, held_date)
left join previous_5_races_avgs using (race_id, horse_id, held_date)
left join previous_1_same_place_races_avgs using (race_id, horse_id, held_date, held_place)
left join previous_same_place_3_races_avgs using (race_id, horse_id, held_date, held_place)
left join previous_same_place_5_races_avgs using (race_id, horse_id, held_date, held_place)
left join previous_1_same_weather_races_avgs using (race_id, horse_id, held_date, weather)
left join previous_same_weather_3_races_avgs using (race_id, horse_id, held_date, weather)
left join previous_same_weather_5_races_avgs using (race_id, horse_id, held_date, weather)
left join previous_1_same_jockey_races_avgs using (race_id, horse_id, held_date, jockey_names)
left join previous_same_jockey_3_races_avgs using (race_id, horse_id, held_date, jockey_names)
left join previous_same_jockey_5_races_avgs using (race_id, horse_id, held_date, jockey_names)
left join previous_1_same_course_type_races_avgs using (race_id, horse_id, held_date, course_type)
left join previous_3_same_course_type_races_avgs using (race_id, horse_id, held_date, course_type)
left join previous_5_same_course_type_races_avgs using (race_id, horse_id, held_date, course_type)
left join previous_1_same_course_condition_races_avgs using (race_id, horse_id, held_date, course_condition)
left join previous_3_same_course_condition_races_avgs using (race_id, horse_id, held_date, course_condition)
left join previous_5_same_course_condition_races_avgs using (race_id, horse_id, held_date, course_condition)
left join previous_1_same_course_direction_races_avgs using (race_id, horse_id, held_date, course_direction)
left join previous_3_same_course_direction_races_avgs using (race_id, horse_id, held_date, course_direction)
left join previous_5_same_course_direction_races_avgs using (race_id, horse_id, held_date, course_direction)
left join previous_1_same_race_number_races_avgs using (race_id, horse_id, held_date)
left join previous_3_same_race_number_races_avgs using (race_id, horse_id, held_date)
left join previous_5_same_race_number_races_avgs using (race_id, horse_id, held_date)
left join previous_1_same_course_length_category_races_avgs using (race_id, horse_id, held_date)
left join previous_3_same_course_length_category_races_avgs using (race_id, horse_id, held_date)
left join previous_5_same_course_length_category_races_avgs using (race_id, horse_id, held_date)
left join previous_1_same_held_month_races_avgs using (race_id, horse_id, held_date)
left join previous_3_same_held_month_races_avgs using (race_id, horse_id, held_date)
left join previous_5_same_held_month_races_avgs using (race_id, horse_id, held_date)
left join previous_1_same_held_day_of_week_races_avgs using (race_id, horse_id, held_date)
left join previous_3_same_held_day_of_week_races_avgs using (race_id, horse_id, held_date)
left join previous_5_same_held_day_of_week_races_avgs using (race_id, horse_id, held_date)
LIMIT 100