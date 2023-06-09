CREATE TYPE eventType as ( team_1 text
                         , team_2 text
                         , map_name text
                         , map_type text
                         , capturing_team text
                         , player_team text
                         , player_name text
                         , player_hero text
                         , previous_hero text
                         , attacker_team text
                         , attacker_name text
                         , attacker_hero text
                         , victim_name text
                         , victim_hero text
                         , victim_team text
                         , event_ability text
                         , healer_team text
                         , healer_name text
                         , healer_hero text
                         , healee_team text
                         , healee_hero text
                         , healee_name text
                         , resurrecter_team text
                         , resurrecter_player text
                         , resurrecter_hero text
                         , resurrectee_team text
                         , resurrectee_player text
                         , resurrectee_hero text
                         , round_number smallint
                         , team_1_score smallint
                         , team_2_score smallint
                         , objective_index smallint
                         , match_time real
                         , match_time_remaining real
                         , point_capture_progress real
                         , payload_capture_progress real
                         , hero_time_played real
                         , hero_duplicated boolean
                         , ultimate_id text
                         , event_damage real
                         , is_critical_hit boolean
                         , is_environmental boolean
                         , event_healing real
                         , is_health_pack boolean
                         , eliminations smallint
                         , final_blows smallint
                         , deaths smallint
                         , all_damage_dealt real
                         , barrier_damage_dealt real
                         , hero_damage_dealt real
                         , healing_dealt real
                         , healing_received real
                         , self_healing real
                         , damage_taken real
                         , damage_blocked real
                         , defensive_assists smallint
                         , offensive_assists smallint
                         , ultimates_earned smallint
                         , ultimates_used smallint
                         , multikill_best smallint
                         , multikills smallint
                         , solo_kills smallint
                         , objective_kills smallint
                         , environmental_kills smallint
                         , environmental_deaths smallint
                         , critical_hits smallint
                         , critical_hit_accuracy real
                         , scoped_accuracy real
                         , scoped_critical_hit_accuracy real
                         , scoped_critical_hit_kills smallint
                         , shots_fired smallint
                         , shots_hit smallint 
                         , shots_missed smallint
                         , scoped_shots_fired smallint
                         , scoped_shots_hit smallint
                         , weapon_accuracy real
                         );