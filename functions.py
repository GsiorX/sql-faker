from faker import Faker
import datetime

error_02 = "List contains to elements."


def get_fake_data(data_target="name", n_rows=100, lang="en_EN", **kwargs):
    """This method generates a certain data item based on the data_target selected
    
    :param data_target: Data item type that should be created
    :type data_target: String
    :param n_rows: Number of rows to generate
    :type n_rows: Integer
    :param lang: Language to be used by Faker
    :type lang: str
    :returns: List of data
    """
    data_faker = Faker()
    # generator_function = getattr(data_faker, data_target)
    # return_list = []
    #
    # for _ in range(n_rows):
    #     if data_target in ["date", "past_datetime", "time", "past_date", "date_time"]:
    #         return_list.append(datetime.datetime.strftime(generator_function(), "%Y-%m-%d %H:%M:%S"))
    #     else:
    #         return_list.append(generator_function())
    # return return_list

    if len(kwargs):
        generator_function = getattr(data_faker, data_target)(**kwargs)
    elif data_target == "":
        generator_function = ""
    else:
        generator_function = getattr(data_faker, data_target)
    return_list = []

    for _ in range(n_rows):
        if data_target == "":
            return_list.append("")
        elif data_target == "simple_profile":
            return_list.append(generator_function()["username"])
        else:
            return_list.append(generator_function())
    return return_list


def check_value_is_not_less_than(value, compare_value):
    """This method checks if a value is larger than a certain value.
    
    :param value: Object to check
    :param compare_value: Threshold to compare with
    """
    if value < compare_value:
        raise ValueError("n_rows must be at least {}, but was {}.".format(
            str(compare_value),
            str(value)
        ))


def check_value_is_not_more_than(value, compare_value):
    """This method checks if a value is smaller than a certain value.
    
    :param value: Object to check
    :param compare_value: Threshold to compare with
    """
    if value > compare_value:
        raise ValueError("n_rows must be at least {}, but was {}.".format(
            str(compare_value),
            str(value)
        ))


def namestr(object):
    """This method returns the string name of a python object.
    
    :param object: The object that should be used
    :type object: Python object
    :returns: Object name as string
    """
    for n, v in globals().items():
        if v == object:
            return n
    return None
