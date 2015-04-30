from pipeline import run

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job(run, CronTrigger(minute=0, hour=1))
    scheduler.start()
