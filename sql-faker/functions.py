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

    generator_function = getattr(data_faker, data_target)
    return_list = []

    for _ in range(n_rows):
        if data_target == "":
            return_list.append("")
        else:
            if len(kwargs):
                return_list.append(generator_function(**kwargs))
            else:
                return_list.append(generator_function())

    return return_list
