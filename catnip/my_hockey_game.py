from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List

from catnip.fla_archtics import FLA_Archtics

@dataclass
class MyHockeyGame:

    event_time : datetime = field(init=False)
    event_opponent : str = field(init=False)
    doors_time : str = field(init=False)
    event_time_formatted : str = field(init=False)
    puckdrop : str = field(init=False)
    final_timestamp : str = field(init=False)
    this_game_filename : str = field(init=False) 
    event_dow : str = field(init=False)

    current_datetime : datetime = datetime.now()
    current_time_formatted : str = datetime.now().strftime('%#I:%M %p')
    current_time : datetime.time = datetime.now().time()
    current_date : str = datetime.now().strftime('%m-%d-%y')
    event_names: List = field(default_factory=list)

    sql_select_statement : str = '''
        SELECT 
            {0}
        FROM 
            [DBA].v_event e 
        WHERE 
            --CAST(e.event_date AS DATE) = CAST(GETDATE() AS DATE)
            CAST(e.event_date AS DATE) = CAST(DATEADD(DAY, 2, CAST(GETDATE() AS DATE)) AS DATE)
            AND e.minor_category IN ('NHL PRO HOCKEY', 'SPORTS:NHL PRO HOCKEY')
            AND e.event_name NOT LIKE '%TEST%'
    '''
            
    def __post_init__(self):

        self.event_opponent = self.get_event_opponent()
        self.event_names = self.get_event_names()
        self.event_time = self.get_event_time()

        self.event_dow = self.current_datetime.strftime('%A')
        self.doors_time = datetime.strptime(str(self.event_time - timedelta(minutes = 45))[11:16],'%H:%M').strftime('%#I:%M %p')
        self.event_time_formatted = datetime.strptime(str(self.event_time)[11:16],'%H:%M').strftime('%#I:%M %p')

        self.puckdrop = datetime.strptime(str(self.event_time + timedelta(minutes = 15))[11:16],'%H:%M')
        self.final_timestamp = datetime.strptime(str(self.puckdrop + timedelta(hours = 1, minutes = 30))[11:16],'%H:%M').strftime('%#I:%M %p')
        self.puckdrop = self.puckdrop.strftime('%#I:%M %p')

        self.this_game_filename  = str(self.event_opponent).replace(" ", "") + "_" + self.current_date + "_TurnstileScans.csv"

    def get_event_opponent(self):

        df = FLA_Archtics().query_database(sql_string = self.sql_select_statement.format("team"))

        return df.iloc[0, 0]

    def get_event_names(self):

        df = FLA_Archtics().query_database(sql_string = self.sql_select_statement.format("event_name"))

        return list(set(list(df['event_name'])))
    
    def get_event_time(self):

        df = FLA_Archtics().query_database(sql_string = self.sql_select_statement.format("event_time"))

        return datetime.strptime(str(df.iloc[0, 0]), '%H:%M:%S')