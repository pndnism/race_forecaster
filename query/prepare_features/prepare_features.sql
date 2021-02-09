#create or replace table horse_race_processed.race_features as 
with 
    race_info as (
        SELECT
            race_id,
            horse_ids as horse_id,
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

    center_race_info as (
        select 
            * 
        from race_info
        where held_place in ("札幌","函館","福島","新潟","中山","東京","中京","京都","阪神","小倉")
        )

select 
    race_id,
    horse_id,
    cast(SPLIT(goal_times, ":")[OFFSET(0)] as float64) * 60 + cast(SPLIT(goal_times, ":")[OFFSET(1)] as float64) as goal_time
from center_race_info