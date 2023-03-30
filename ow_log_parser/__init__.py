import csv
from sqlalchemy import Integer, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import create_engine
import os
import datetime

event_data = {
    'match_start': [ 'match_time', 'map_name', 'map_type', 'team_1', 'team_2' ],
    'match_end': [ 'match_time', 'round_number', 'team_1_score', 'team_2_score' ],
    'round_start': [ 'match_time', 'round_number', 'capturing_team', 'team_1_score', 'team_2_score', 'objective_index' ],
    'round_end': [ 'match_time', 'round_number', 'capturing_team', 'team_1_score', 'team_2_score', 'objective_index', 'control_team_1_progress', 'control_team_2_progress', 'match_time_remaining' ],
    'setup_complete': [ 'match_time', 'round_number', 'match_time_remaining' ],
    'objective_captured': [ 'match_time', 'round_number', 'capturing_team', 'objective_index', 'control_team_1_progress', 'control_team_2_progress', 'match_time_remaining' ],
    'point_progress': [ 'match_time', 'round_number', 'capturing_team', 'objective_index', 'point_capture_progress' ],
    'payload_progress': [ 'match_time', 'round_number', 'capturing_team', 'objective_index', 'payload_capture_progress'],
    'hero_spawn': [ 'match_time', 'player_team', 'player_name', 'player_hero', 'previous_hero', 'hero_time_played' ],
    'hero_swap': [ 'match_time', 'player_team', 'player_name', 'player_hero', 'previous_hero', 'hero_time_played' ],
    'ability_1_used': [ 'match_time', 'player_team', 'player_name', 'player_hero', 'hero_duplicated' ],
    'ability_2_used': [ 'match_time', 'player_team', 'player_name', 'player_hero', 'hero_duplicated' ],
    'offensive_assist': [ 'match_time', 'player_team', 'player_name', 'player_hero', 'hero_duplicated' ],
    'defensive_assist': [ 'match_time', 'player_team', 'player_name', 'player_hero', 'hero_duplicated' ],
    'ultimate_charged': [ 'match_time', 'player_team', 'player_name', 'player_hero', 'hero_duplicated', 'ultimate_id' ],
    'ultimate_start': [ 'match_time', 'player_team', 'player_name', 'player_hero', 'hero_duplicated', 'ultimate_id' ],
    'ultimate_end': [ 'match_time', 'player_team', 'player_name', 'player_hero', 'hero_duplicated', 'ultimate_id' ],
    'kill': [ 'match_time', 'attacker_team', 'attacker_name', 'attacker_hero', 'victim_team', 'victim_name', 'victim_hero', 'event_ability', 'event_damage', 'is_critical_hit', 'is_environmental'],
    'damage': [ 'match_time', 'attacker_team', 'attacker_name', 'attacker_hero', 'victim_team', 'victim_name', 'victim_hero', 'event_ability', 'event_damage', 'is_critical_hit', 'is_environmental'],
    'healing': [ 'match_time', 'healer_team', 'healer_name', 'healer_hero', 'healee_team', 'healee_name', 'healee_hero', 'event_ability', 'event_healing', 'is_health_pack' ],
    'mercy_rez': [ 'match_time', 'resurrecter_team', 'resurrecter_player', 'resurrecter_hero','resurrectee_team', 'resurrectee_player', 'resurrectee_hero' ],
    'echo_duplicate_start': [ 'match_time', 'player_team', 'player_name', 'player_hero', 'hero_duplicated', 'ultimate_id' ],
    'echo_duplicate_end': [ 'match_time', 'player_team', 'player_name', 'player_hero', 'ultimate_id' ],
    'dva_demech': [ 'match_time', 'attacker_team', 'attacker_name', 'attacker_hero', 'victim_team', 'victim_name', 'victim_hero', 'event_ability', 'event_damage', 'is_critical_hit', 'is_environmental' ],
    'dva_remech': [ 'match_time', 'player_team', 'player_name', 'player_hero', 'ultimate_id' ],
    'remech_charged': [ 'match_time', 'player_team', 'player_name', 'player_hero', 'hero_duplicated' ],
    'player_stat': [ 'match_time', 'round_number', 'player_team', 'player_name', 'player_hero', 'eliminations', 'final_blows', 'deaths', 'all_damage_dealt', 'barrier_damage_dealt', 'hero_damage_dealt', 'healing_dealt', 'healing_received', 'self_healing', 'damage_taken', 'damage_blocked', 'defensive_assists', 'offensive_assists', 'ultimates_earned', 'ultimates_used', 'multikill_best', 'multikills', 'solo_kills', 'objective_kills', 'environmental_kills', 'environmental_deaths', 'critical_hits', 'critical_hit_accuracy', 'scoped_accuracy', 'scoped_critical_hit_accuracy', 'scoped_critical_hit_kills', 'shots_fired', 'shots_hit', 'shots_missed', 'scoped_shots_fired', 'scoped_shots_hit', 'weapon_accuracy', 'hero_time_played' ]
}

is_str = [ 'map_name', 'map_type', 'team_1', 'team_2', 'capturing_team', 'player_team', 'player_name', 'player_hero', 'previous_hero', 
           'attacker_team', 'attacker_name', 'attacker_hero', 'victim_name', 'victim_hero', 'victim_team', 'event_ability', 'healer_team',
           'healer_name', 'healer_hero', 'healee_team', 'healee_hero', 'healee_name', 'resurrecter_team', 'resurrecter_player', 
           'resurrecter_hero','resurrectee_team', 'resurrectee_player', 'resurrectee_hero' ]
is_int = [ 'round_number', 'team_1_score', 'team_2_score', 'objective_index', 'eliminations',
           'final_blows', 'deaths', 'defensive_assists', 'offensive_assists', 'ultimates_earned',
            'ultimates_used', 'multikill_best', 'multikills', 'solo_kills', 'objective_kills',
             'environmental_kills', 'environmental_deaths', 'critical_hits', 'scoped_critical_hit_kills',
             'shots_fired', 'shots_hit', 'shots_missed', 'scoped_shots_fired', 'scoped_shots_hit' ]
is_float = [ 'match_time', 'match_time_remaining', 'point_capture_progress', 
             'payload_capture_progress', 'hero_time_played', 'event_damage', 
             'event_healing', 'all_damage_dealt', 'barrier_damage_dealt', 'hero_damage_dealt',
             'healing_dealt', 'healing_received', 'self_healing', 'damage_taken', 'damage_blocked',
             'critical_hit_accuracy', 'scoped_accuracy', 'scoped_critical_hit_accuracy', 'weapon_accuracy' ]
is_bool = [ 'hero_duplicated', 'is_critical_hit', 'is_environmental', 'is_health_pack' ]

class Base(DeclarativeBase):
    pass

class MatchEvent(Base):
    __tablename__ = 'event'

    id: Mapped[int] = mapped_column(primary_key=True)
    event_type: Mapped[str]
    match_id: Mapped[int]
    event = mapped_column(JSONB)
    timestamp: Mapped[datetime.datetime]

def mkEngine(url):
    return create_engine(url, echo=False)

def parse_file(path):
    out = {}
    for k in event_data:
        out[k] = []
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        print(reader)
        for row in reader:
            t = row[0]
            eType = row[1]
            data = row[2:]
            if eType in event_data:
                p = {}
                for i in range(len(event_data[eType])):
                    field = event_data[eType][i]
                    val = data[i]
                    if(field in is_int):
                        val = int(val)
                    elif(field in is_float):
                        val = float(val)
                    elif(field in is_bool):
                        val = bool(val)
                    p[field] = val
                out[eType].append({
                    'time': t,
                    'data': p
                })
    return out

def create_fake_match(s):
    for t in event_data:
        data = {}
        for i in range(len(event_data[t])):
            field = event_data[t][i]
            val = None
            if(field in is_str):
                val = "NONE"
            elif(field in is_int):
                val = -1
            elif(field in is_float):
                val = 1.2
            elif(field in is_bool):
                val = True
            else:
                val = "NONE"
            data[field] = val
        evt = MatchEvent(event_type = t, match_id=0, event = data)
        s.add(evt)
    s.commit()

def insert_parsed(engine, parsed, date):
    with Session(engine) as s:
        match_id = s.query(func.max(MatchEvent.match_id)).scalar()+1
        for t in parsed:
            for e in parsed[t]:
                evt = MatchEvent(event_type = t, event = e['data'], match_id=match_id, timestamp=date)
                s.add(evt)
        s.commit()


def main():
    engine = mkEngine("postgresql+psycopg2://postgres:postgrespw@localhost:49154")
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        if s.query(MatchEvent).count() == 0:
            create_fake_match(s)

    directory = os.fsencode('logs')
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename.endswith('.txt'):
            path = os.path.join(directory, file)
            date = datetime.datetime.strptime(filename[4:-5],"%Y-%m-%d-%H-%M-%S")
            parsed = parse_file(path)
            insert_parsed(engine, parsed, date)

if __name__ == '__main__':
    main()