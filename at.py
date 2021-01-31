import time

import pandas as pd
import numpy as np

from connections import replication

pd.set_option('display.max_columns', 29)

# It takes a string as argument and returns a string, which is a sql statement.
def sql(who):

    return f'''with t as (

               select attendance.id attendance_id
                      , employee.name
--                      , to_char(
--                           (attendance.check_in at time zone 'utc' at time zone 'nz')
--                           , 'HH24:MM:DD'
--                        ) check_in
                      , substring(
                            (attendance.check_in at time zone 'utc' at time zone 'nz')::text
                            , '..:..:..'
                        ) check_in
                      , substring( 
                            coalesce (
                                ( attendance.check_out at time zone 'utc' at time zone 'nz')::text
                                , (now() at time zone 'utc' at time zone 'utc' at time zone 'nz')::text
                            )
                            , '..:..:..'
                        ) check_out
                      , extract(
                            epoch from (attendance.check_out - attendance.check_in )
                            )/3600 - worked_hours = 0 "QC"
                      , case when attendance.check_out is null then 'working'
                             else 'OK'
                         end status
                      , round(
                            extract(
                                epoch from (
                                    coalesce( attendance.check_out, now() at time zone 'utc')
                                        - (attendance.check_in)
                                    )/3600 )::numeric
                            , 2
                        ) hours

                      , date(attendance.check_in at time zone 'utc' at time zone 'nz') a_date
                      , to_char(
                            date(attendance.check_in at time zone 'utc' at time zone 'nz')
                            , 'IYYY-IW'
                        ) a_week
                        
                 from hr_attendance attendance
                      left join hr_employee employee on attendance.employee_id = employee.id

                where employee.name ~* '{who}'

                order by attendance.id desc

              )

              select *
                     , sum(hours) over(partition by a_date order by attendance_id) hours_of_day
                     , sum(hours) over(partition by a_week order by attendance_id) hours_of_week
                from t
               order by a_date desc, attendance_id desc
    '''

who = str(input('')) or 'rick'
df = pd.read_sql(sql(who), replication)

print(df.head(10))
print()

time.sleep(5)
