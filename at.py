import time

import pandas as pd
import numpy as np

from connections import replication

pd.set_option('display.max_columns', 29)

# It takes a string as argument and returns a string, which is a sql statement.
def sql(who):

    return f'''select he.name
                      , to_char(
                           (ha.check_in at time zone 'utc' at time zone 'nz')
                           , 'HH24:MM:DD'
                        ) check_in
                      , to_char(
                            coalesce( ha.check_out at time zone 'utc' at time zone 'nz'
                                , substring( (now() at time zone 'nz')::text
                                    , '(^.+)\.'
                                )::timestamp
                                )
                             , 'HH24:MM:DD'
                        ) check_out
                      , extract(
                            epoch from (check_out - check_in )
                            )/3600 - worked_hours = 0 "QC"
                      , case when ha.check_out is null then 'working'
                             else 'OK'
                         end status
                      , ( coalesce( ha.check_out at time zone 'utc' at time zone 'nz'
                          , substring( (now() at time zone 'nz')::text
                                , '(^.+)\.'
                            )::timestamp
                         ) - (ha.check_in at time zone 'utc' at time zone 'nz') ) hours

                      , date(ha.check_in at time zone 'utc' at time zone 'nz') date
                      , to_char(
                            date(ha.check_in at time zone 'utc' at time zone 'nz')
                            , 'IYYY-IW'
                        ) week
                        
                 from hr_attendance ha
                      left join hr_employee he on ha.employee_id = he.id

                where he.name ~* '{who}'

                order by ha.id desc
    '''

who = str(input('')) or 'rick'
df = pd.read_sql(sql(who), replication)

print(df.head())
weekly_hours = df.groupby(['name','week']).hours.sum().reset_index()

weekly_hours['hours'] = round(weekly_hours['hours']/np.timedelta64(1, 'h'), 2)

print()
print(weekly_hours.tail(2))

time.sleep(5)
