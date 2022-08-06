
from datetime import datetime
import pytz
 
table = "table_"
 #current date and time
now = datetime.utcnow()
now = datetime.now(pytz.timezone('US/Eastern'))
format = "%Y%m%d"
#format datetime using strftime() 
table_suffix = now.strftime(format)
table = table + table_suffix
print("Formatted Date:", table)

# gaanalytics-358212.analytics_322371309.events_intraday_20220806
# gaanalytics-358212.analytics_322371309.events_intraday_20220806 
# gaanalytics-358212.analytics_322371309.events_intraday_20220805