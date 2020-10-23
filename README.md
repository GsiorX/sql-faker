# Data Structure Creation with `sql-faker`

`sql-faker` is a python library that can be used to generate relational data structures and fill these structures with fake data.
It is built upon python library [sqlfaker](https://pypi.org/project/sqlfaker/) made by [kohleggermichael](https://pypi.org/user/kohleggermichael).
`sql-faker` features Oracle SQL export support, enhanced handling of [Faker](https://github.com/joke2k/faker) methods and better foreign key generation.

## Installation
Coming soon

## Class structure

This project lets you define relational data structures that are build upon the concepts of `Database`, `Table` and `Column`.

A `Database` can have multiple `Table` objects which again can each have multiple `Column` objects.
There are two classes that inherit from the `Column` class - `ForeignKey` and `PrimaryKey`. These allow you to create key columns.

# Credits
Made possible thanks to [kohleggermichael](https://pypi.org/user/kohleggermichael)
and his package [sqlfaker](https://pypi.org/project/sqlfaker/)
