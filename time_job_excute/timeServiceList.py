class TimeService:
    __hour_service = []
    __day_service = []
    __week_mon_service = []
    __month_first_service = []
    __quarter_first_service = []
    __more_days_service = []
    __gt_newconsumers_service = []
    __hard_service = []
    __localytics_service = []
    __later_service = []

    @staticmethod
    def add_hour_service(job_fun):
        TimeService.__hour_service.append(job_fun)

    @staticmethod
    def add_day_service(job_fun):
        TimeService.__day_service.append(job_fun)

    @staticmethod
    def add_week_mon_service(job_fun):
        TimeService.__week_mon_service.append(job_fun)

    @staticmethod
    def add_quarter_first_service(job_fun):
        TimeService.__quarter_first_service.append(job_fun)

    @staticmethod
    def add_month_first_service(job_fun):
        TimeService.__month_first_service.append(job_fun)

    @staticmethod
    def add_more_days_service(job_fun):
        TimeService.__more_days_service.append(job_fun)

    @staticmethod
    def add_gt_newconsumers_service(job_fun):
        TimeService.__gt_newconsumers_service.append(job_fun)

    @staticmethod
    def add_hard_service(job_fun):
        TimeService.__hard_service.append(job_fun)

    @staticmethod
    def add_localytics_service(job_fun):
        TimeService.__localytics_service.append(job_fun)

    @staticmethod
    def add_later_service(job_fun):
        TimeService.__later_service.append(job_fun)

    @staticmethod
    def get_hour_service():
        return TimeService.__hour_service

    @staticmethod
    def get_day_service():
        return TimeService.__day_service

    @staticmethod
    def get_week_mon_service():
        return TimeService.__week_mon_service

    @staticmethod
    def get_quarter_first_service():
        return TimeService.__quarter_first_service

    @staticmethod
    def get_month_first_service():
        return TimeService.__month_first_service

    @staticmethod
    def get_more_days_service():
        return TimeService.__more_days_service

    @staticmethod
    def get_gt_newconsumers_service():
        return TimeService.__gt_newconsumers_service

    @staticmethod
    def get_hard_service():
        return TimeService.__hard_service

    @staticmethod
    def get_localytics_service():
        return TimeService.__localytics_service

    @staticmethod
    def get_later_service():
        return TimeService.__later_service




