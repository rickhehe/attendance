import ctypes
from datetime import timedelta, datetime
import pandas as pd
from connections import replication

pd.set_option('display.max_columns', 29)

# It takes a string as argument and returns a string, which is a sql statement.
def sql(who):

    return f'''select he.name
                      ,(ha.check_in at time zone 'utc' at time zone 'nz') check_in
                      ,(ha.check_out at time zone 'utc' at time zone 'nz') check_out
                      ,date(ha.check_in at time zone 'utc' at time zone 'nz') date
                      ,he.pin pin
                 from hr_attendance ha
                      left join hr_employee he on ha.employee_id = he.id
                where he.name ~* '{who}'
                order by ha.id desc
                limit 100'''

def fake_strftime(td):
    '''
    Td stands for timedelta.

    '''
    tds = td.total_seconds()
    hours, remainder = divmod(tds, 3600)
    minutes, seconds = divmod(remainder, 60)
    hehe = f'{hours:.0f}:{minutes:2.0f}:{seconds:2.0f}'#, int(minutes), int(seconds))'
    return hehe


who = str(input('')) or 'rick'
df = pd.read_sql(sql(who), replication)

print(df.head(10))

# If in the top row, check out is blank, then your time is making money.
if df.check_out.isna()[0]:
    msg = 'wow you are making a fortune'
else:
    msg = 'what are you looking at?'


df = df.fillna(datetime.now())
df['hours'] = df['check_out'] - df['check_in']
df['week'] = df['check_in'].dt.week

weekly_hours = df.groupby(['name','week']).hours.sum().reset_index()
weekly_hours['hours'] = weekly_hours['hours'].map(fake_strftime)

print()
print(weekly_hours.tail(2))

daily_hours = df.groupby(['name','date']).hours.sum().reset_index()
daily_hours['hours'] = daily_hours['hours'].map(fake_strftime)

print(daily_hours.tail(2))

ctypes.windll.user32.MessageBoxW(
    0,
    msg,
    who,
    1,
)
