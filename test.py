# add database
from database import Database

my_db = Database(db_name="C##ZSBD")

# add tables
my_db.add_table(table_name="CrewMember", n_rows=5)
my_db.add_table(table_name="Genre", n_rows=5)
my_db.add_table(table_name="Country", n_rows=500)
my_db.add_table(table_name="Award", n_rows=5)
my_db.add_table(table_name="Job", n_rows=3)
my_db.add_table(table_name="Customer", n_rows=3)
my_db.add_table(table_name="Translation", n_rows=3)
my_db.add_table(table_name="Language", n_rows=3)
my_db.add_table(table_name="Movie", n_rows=3)
my_db.add_table(table_name="Rent", n_rows=3)
my_db.add_table(table_name="Role", n_rows=3)
my_db.add_table(table_name="Rate", n_rows=3)
my_db.add_table(table_name="Genre_Movie", n_rows=3)
my_db.add_table(table_name="Movie_Award", n_rows=3)
my_db.add_table(table_name="Country_Movie", n_rows=3)
my_db.add_table(table_name="CrewMember_Award", n_rows=3)
my_db.add_table(table_name="CrewMember_Job", n_rows=3)
my_db.add_table(table_name="CrewMember_Movie", n_rows=3)

# add columns to CrewMember table

my_db.tables["CrewMember"].add_primary_key(column_name="CrewMemberId")
my_db.tables["CrewMember"].add_column(column_name="Name", data_target="first_name", data_type="varchar(255)")
my_db.tables["CrewMember"].add_column(column_name="Surname", data_target="last_name", data_type="varchar(255)")
my_db.tables["CrewMember"].add_column(column_name="Gender", data_target="bothify", data_type="char(1)", text="?",
                                      letters="MF")
my_db.tables["CrewMember"].add_column(column_name="BirthDate", data_target="date", data_type="date")
my_db.tables["CrewMember"].add_column(column_name="Height", data_target="random_int", data_type="number(10)",
                                      min=150, max=210)

my_db.tables["Movie"].add_primary_key(column_name="MovieId")
my_db.tables["Movie"].add_column(column_name="Title", data_target="sentence", data_type="varchar(255)",
                                 nb_words=3)
my_db.tables["Movie"].add_column(column_name="PremiereDate", data_target="date", data_type="date")
my_db.tables["Movie"].add_column(column_name="Duration", data_target="random_int", data_type="number(10)",
                                 min=30, max=360)
my_db.tables["Movie"].add_column(column_name="Budget", data_target="pyfloat", data_type="float(10)",
                                 left_digits=None, right_digits=2, positive=True, min_value=200000,
                                 max_value=9999999)
my_db.tables["Movie"].add_column(column_name="Description", data_target="paragraph", data_type="varchar(255)",
                                 nb_sentences=3, variable_nb_sentences=True)
my_db.tables["Movie"].add_column(column_name="Studio", data_target="word", data_type="varchar(255)")
my_db.tables["Movie"].add_column(column_name="Promo", data_target="pyfloat", data_type="float(10)",
                                 left_digits=None, right_digits=2, positive=True, min_value=0, max_value=1)

my_db.tables["Genre"].add_primary_key(column_name="GenreId")
my_db.tables["Genre"].add_column(column_name="Name", data_target="word", data_type="varchar(255)",
                                 ext_word_list=["Action", "Adventure", "Animation", "Comedy", "Crime",
                                                "Documentary", "Drama",
                                                "Family", "Fantasy", "Foreign", "History", "Horror", "Music",
                                                "Mystery",
                                                "Romance", "Science Fiction", "Thriller", "War", "Western"])

my_db.tables["Role"].add_primary_key(column_name="RoleId")
my_db.tables["Role"].add_column(column_name="Name", data_target="name", data_type="varchar(255)")

my_db.tables["Country"].add_primary_key(column_name="CountryId")
my_db.tables["Country"].add_column(column_name="Name", data_target="country", data_type="varchar(255)")

my_db.tables["Award"].add_primary_key(column_name="AwardId")
my_db.tables["Award"].add_column(column_name="Name", data_target="country", data_type="varchar(255)")
my_db.tables["Award"].add_column(column_name="Category", data_target="word", data_type="varchar(255)")
my_db.tables["Award"].add_column(column_name="DeliveryDate", data_target="date", data_type="date")
my_db.tables["Award"].add_column(column_name="IsWinner", data_target="pyint", data_type="number", min_value=0,
                                 max_value=1)

my_db.tables["Job"].add_primary_key(column_name="JobId")
my_db.tables["Job"].add_column(column_name="Name", data_target="name", data_type="varchar(255)")

my_db.tables["Rate"].add_primary_key(column_name="RateId")
my_db.tables["Rate"].add_column(column_name="Value", data_target="pyint", data_type="number(10)", min_value=0,
                                max_value=10)
my_db.tables["Rate"].add_column(column_name="Title", data_target="sentence", data_type="varchar(255)",
                                nb_words=3)
my_db.tables["Rate"].add_column(column_name="Description", data_target="paragraph", data_type="varchar(255)",
                                nb_sentences=3, variable_nb_sentences=True)
my_db.tables["Rate"].add_column(column_name="RateDate", data_target="date", data_type="date")
my_db.tables["Rate"].add_column(column_name="Verified", data_target="pyint", data_type="number", min_value=0,
                                max_value=1)
my_db.tables["Rate"].add_column(column_name="Views", data_target="random_int", data_type="number(10)", min=30,
                                max=360)

my_db.tables["Customer"].add_primary_key(column_name="CustomerId")
my_db.tables["Customer"].add_column(column_name="Email", data_target="ascii_safe_email",
                                    data_type="varchar(255)")
my_db.tables["Customer"].add_column(column_name="Login", data_target="first_name", data_type="varchar(255)")
my_db.tables["Customer"].add_column(column_name="Password", data_target="password", data_type="varchar(255)")
my_db.tables["Customer"].add_column(column_name="RegistrationDate", data_target="date", data_type="date")
my_db.tables["Customer"].add_column(column_name="BirthDate", data_target="date", data_type="date")
my_db.tables["Customer"].add_column(column_name="FreeMovies", data_target="random_int", data_type="number(10)",
                                    min=0, max=5)
my_db.tables["Customer"].add_column(column_name="EmailVerified", data_target="pyint", data_type="number",
                                    min_value=0, max_value=1)

my_db.tables["Rent"].add_primary_key(column_name="RentId")
my_db.tables["Rent"].add_column(column_name="RentDate", data_target="date", data_type="date")
my_db.tables["Rent"].add_column(column_name="Price", data_target="pyfloat", data_type="float(10)",
                                left_digits=None, right_digits=2, positive=True, min_value=200000,
                                max_value=9999999)
my_db.tables["Rent"].add_column(column_name="Quality", data_target="random_int", data_type="number(10)", min=0,
                                max=5)  # TODO to enum?
my_db.tables["Rent"].add_column(column_name="Period", data_target="random_int", data_type="number(10)", min=30,
                                max=360)
my_db.tables["Rent"].add_column(column_name="Promo", data_target="pyfloat", data_type="float(10)",
                                left_digits=None, right_digits=2, positive=True, min_value=0, max_value=1)

my_db.tables["Translation"].add_primary_key(column_name="TranslationId")
my_db.tables["Translation"].add_column(column_name="Type", data_target="random_int", data_type="number(10)",
                                       min=0, max=3)  # TODO to enum?
my_db.tables["Translation"].add_column(column_name="TranslationDate", data_target="date", data_type="date")
my_db.tables["Translation"].add_column(column_name="Author", data_target="name", data_type="varchar(255)")
my_db.tables["Translation"].add_column(column_name="Copyright", data_target="word", data_type="varchar(255)")
my_db.tables["Translation"].add_column(column_name="RentRentId", data_target="", data_type="number(10)")

my_db.tables["Language"].add_primary_key(column_name="LanguageId")
my_db.tables["Language"].add_column(column_name="Name", data_target="language_name", data_type="varchar(255)")

# Foreign keys - intermediate tables
my_db.tables["Genre_Movie"].add_foreign_key(column_name="GenreGenreId", target_table="Genre", target_column="GenreId")
my_db.tables["Genre_Movie"].add_foreign_key(column_name="MovieMovieId", target_table="Movie", target_column="MovieId")
my_db.tables["Movie_Award"].add_foreign_key(column_name="MovieMovieId", target_table="Movie", target_column="MovieId")
my_db.tables["Movie_Award"].add_foreign_key(column_name="AwardAwardId", target_table="Award", target_column="AwardId")
my_db.tables["Country_Movie"].add_foreign_key(column_name="CountryCountryId", target_table="Country",
                                              target_column="CountryId")
my_db.tables["Country_Movie"].add_foreign_key(column_name="MovieMovieId", target_table="Movie", target_column="MovieId")
my_db.tables["CrewMember_Award"].add_foreign_key(column_name="CrewMemberCrewMemberId", target_table="CrewMember",
                                                 target_column="CrewMemberId")
my_db.tables["CrewMember_Award"].add_foreign_key(column_name="AwardAwardId", target_table="Award",
                                                 target_column="AwardId")
my_db.tables["CrewMember_Job"].add_foreign_key(column_name="CrewMemberCrewMemberId", target_table="CrewMember",
                                               target_column="CrewMemberId")
my_db.tables["CrewMember_Job"].add_foreign_key(column_name="JobJobId", target_table="Job", target_column="JobId")
my_db.tables["CrewMember_Movie"].add_foreign_key(column_name="CrewMemberCrewMemberId", target_table="CrewMember",
                                                 target_column="CrewMemberId")
my_db.tables["CrewMember_Movie"].add_foreign_key(column_name="MovieMovieId", target_table="Movie",
                                                 target_column="MovieId")

# Foreign keys
my_db.tables["Movie"].add_foreign_key(column_name="TranslationTranslationId", target_table="Translation",
                                      target_column="TranslationId")
my_db.tables["Role"].add_foreign_key(column_name="MovieMovieId", target_table="Movie", target_column="MovieId")
my_db.tables["Role"].add_foreign_key(column_name="CrewMemberCrewMemberId", target_table="CrewMember",
                                     target_column="CrewMemberId")
my_db.tables["Country"].add_foreign_key(column_name="CrewMemberCrewMemberId", target_table="CrewMember",
                                        target_column="CrewMemberId")
my_db.tables["Rate"].add_foreign_key(column_name="MovieMovieId", target_table="Movie", target_column="MovieId")
my_db.tables["Rate"].add_foreign_key(column_name="CustomerCustomerId", target_table="Customer",
                                     target_column="CustomerId")
my_db.tables["Rent"].add_foreign_key(column_name="CustomerCustomerId", target_table="Customer",
                                     target_column="CustomerId")
my_db.tables["Rent"].add_foreign_key(column_name="MovieMovieId", target_table="Movie", target_column="MovieId")

my_db.generate_data()
my_db.export_sql("generated.sql")
