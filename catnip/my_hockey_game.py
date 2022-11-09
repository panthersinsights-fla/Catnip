from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List

from catnip.fla_redshift import FLA_Redshift

@dataclass
class MyHockeyGame:

    event_time : datetime = field(init=False)
    event_opponent : str = field(init=False)
    doors_time : str = field(init=False)
    event_time_formatted : str = field(init=False)
    puckdrop : str = field(init=False)
    final_timestamp : str = field(init=False)
    event_dow : str = field(init=False)

    current_datetime : datetime = datetime.now() - timedelta(hours = 5)
    current_time_formatted : str = datetime.strftime(datetime.now() - timedelta(hours = 5), '%#I:%M %p')
    current_time : datetime.time = (datetime.now() - timedelta(hours = 5)).time()
    current_date : str = datetime.strftime(datetime.now() - timedelta(hours = 5), '%m-%d-%y')
    event_names: List = field(default_factory=list)

    sql_select_statement : str = '''
        SELECT 
            {0}
        FROM 
            custom.arch_cth_raw_v_event e
        WHERE
            to_date(event_date, 'YYYY-MM-DD') = CAST(GETDATE() AS DATE)
            --to_date(event_date, 'YYYY-MM-DD') = CAST(dateadd(DAY, 7, getdate()) AS DATE)
            AND minor_category IN ('NHL PRO HOCKEY', 'SPORTS:NHL PRO HOCKEY')
            AND event_name NOT LIKE '%TEST%'
            AND season_id = 355
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

    def get_event_opponent(self):

        df = FLA_Redshift().query_warehouse(sql_string = self.sql_select_statement.format("team"))

        return df.iloc[0, 0]

    def get_event_names(self):

        df = FLA_Redshift().query_warehouse(sql_string = self.sql_select_statement.format("event_name"))

        return list(set(list(df['event_name'])))
    
    def get_event_time(self):

        df = FLA_Redshift().query_warehouse(sql_string = self.sql_select_statement.format("event_time"))

        return datetime.strptime(str(df.iloc[0, 0]), '%H:%M:%S')