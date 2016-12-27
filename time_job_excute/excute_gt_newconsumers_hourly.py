from main_service.gt import gt_newconsumers
from time_job_excute.timeServiceList import TimeService
import logging


#8-24
if __name__ == "__main__":
    TimeService.add_gt_newconsumers_service(gt_newconsumers.gt_newconsumers_hourly)
    for fun in TimeService.get_gt_newconsumers_service():
        try:
            fun()
        except Exception as e:
            logging.warning(e.message)
            continue



