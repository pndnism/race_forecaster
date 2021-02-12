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
    base_categorical_features as (
        select 
            race_id,
            horse_id,
            box_numbers,
            horse_numbers,
            horse_sexes,
            jockey_names,
            trainer_names,
            horse_owners,
            race_number,
            course_type,
            course_direction,
            weather,
            course_condition,
            race_start_time,
            REGEXP_EXTRACT(held_date, r'\d+月') as held_month,
            EXTRACT(DAYOFWEEK from DATE(cast(REGEXP_EXTRACT(held_date, r'(\d+)年') as int64), cast(REGEXP_EXTRACT(held_date, r'(\d+)月') as int64),cast(REGEXP_EXTRACT(held_date, r'(\d+)日') as int64))) as held_day_of_week,
            held_place,
            case 
              when course_length < 1400 then '短距離'
              when course_length < 1800 then 'マイル'
              when course_length < 2200 then '中距離'
              when course_length < 2800 then '中長距離'
              else '長距離'
            end course_length_category,
            producer,
            origin_place,
            mother,
            father,
            mother_of_mother,
            father_of_mother,
            mother_of_father,
            father_of_father
            from center_race_info
            left outer join horse_info
            using (horse_id)
    )

select * from base_categorical_features LIMIT 1000

# select course_length, count(*) from center_race_info
# group by course_length
# order by course_length
