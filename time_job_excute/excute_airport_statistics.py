from main_service.hb import air_convert
import logging

#8-24
if __name__ == "__main__":
    try:
        air_convert.airport_info_covert_hourly()
    except Exception as e:
        logging.warning("airport_statistics load error")
