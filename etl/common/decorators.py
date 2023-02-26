import logging
from functools import wraps
from time import sleep


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as err:
                    logging.error(err)
                    sleep(t)
                    if t < border_sleep_time:
                        t += start_sleep_time * 2 ** (factor)
                    else:
                        t = border_sleep_time

        return inner

    return func_wrapper
