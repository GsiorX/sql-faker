from faker import Faker


def get_fake_data(kwargs, data_target="name", n_rows=100, lang="en_US"):
    """This method generates a certain data item based on the data_target selected
    
    :param kwargs:
    :param data_target: Data item type that should be created
    :type data_target: String
    :param n_rows: Number of rows to generate
    :type n_rows: Integer
    :param lang: Language to be used by Faker
    :type lang: str
    :returns: List of data
    """
    data_faker = Faker(lang)

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
        else:
            if callable(generator_function):
                return_list.append(generator_function())
            else:
                return_list.append(generator_function)

    return return_list
