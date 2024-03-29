import os, time
from datetime import datetime
import schedule
import run_cont_ingestion_job 


def job():
    print(f"running continous ingestion: {datetime.now()}")
    run_cont_ingestion_job.main()


schedule.every().day.at("10:56").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)