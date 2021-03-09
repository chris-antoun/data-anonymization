# Databricks notebook source
# MAGIC %run ../../setup

# COMMAND ----------

# DBTITLE 1,Import needed libraries
from faker import Faker

# COMMAND ----------


class Anonymization:
    def __init__(self, env: str):
        self.env = env

    def get_sensitive_field_list(self) -> list:
        """This function returns the list of all data fields that currently support anonymization."""
        return [
            "name",
            "firstname",
            "lastname",
            "email",
            "city",
            "postalcode",
            "countrycode",
            "dateofbirth",
            "telephonenumber",
            "mobilenumber",
            "state",
            "building",
            "title",
            "address",
        ]

    def create_fake_name(self, name: str) -> str:
        """This function anonymizes a name string."""
        fake = Faker()
        return fake.name_nonbinary()

    def create_fake_first_name(self, first_name: str) -> str:
        """This function anonymizes a first name string."""
        fake = Faker()
        return fake.first_name()

    def create_fake_last_name(self, last_name: str) -> str:
        """This function anonymizes a last name string."""
        fake = Faker()
        return fake.last_name()

    def create_fake_date_of_birth(self, dob: date) -> date:
        """This function anonymizes a date of birth input."""
        fake = Faker()
        return fake.date_of_birth()

    def create_fake_email(self, email: str) -> str:
        """This function anonymizes an email"""
        fake = Faker()
        return fake.email()

    def create_fake_country_code(self, code: str) -> str:
        """This function anonymizes a country code"""
        fake = Faker()
        return fake.country_code()

    def create_fake_city(self, city: str) -> str:
        """This function anonymizes a city name"""
        fake = Faker()
        return fake.city()

    def create_fake_postal_code(self, code: str) -> str:
        """This function anonymizes a postal code"""
        fake = Faker()
        return fake.postalcode()

    def create_fake_phone_number(self, number: str) -> str:
        """This function anonymizes a telephone number"""
        fake = Faker()
        return fake.phone_number()

    def create_fake_state(self, state: str) -> str:
        """This function anonymizes a state"""
        fake = Faker()
        return fake.state()

    def create_fake_building(self, building: str) -> str:
        """This function anonymizes a building number"""
        fake = Faker()
        return fake.building_number()

    def create_fake_title(self, prefix: str) -> str:
        """This function anonymizes a name's title (e.g. Mr, Mx, Dr., Ms, etc.)"""
        fake = Faker()
        return fake.prefix_nonbinary()

    def create_fake_address(self, address: str) -> str:
        """This function anonymizes a given address"""
        fake = Faker()
        return fake.address()

    def get_corresponding_function(self, column_name: str):
        """This function takes a string as an input representing a sensitive information column name. It outputs the corresponding function to anonymize the column. It acts as a case (switch) command."""
        # Define dictionary
        sensitive_field_dict = {
            "firstname": self.create_fake_first_name,
            "lastname": self.create_fake_last_name,
            "email": self.create_fake_email,
            "city": self.create_fake_city,
            "postalcode": self.create_fake_postal_code,
            "countrycode": self.create_fake_country_code,
            "dateofbirth": self.create_fake_date_of_birth,
            "telephonenumber": self.create_fake_phone_number,
            "mobilenumber": self.create_fake_phone_number,
            "state": self.create_fake_state,
            "building": self.create_fake_building,
            "title": self.create_fake_title,
            "name": self.create_fake_name,
            "address": self.create_fake_address,
        }
        return sensitive_field_dict.get(column_name.lower())

    def anonymize_sensitive_column(self, dataframe, sensitive_column_name: str):
        """
    This function takes a dataframe (DataFrame) and a column name (string) as inputs. It anonymizes the rows of the given column and outputs the altered dataframe.
    Example usage:
    customer_df = anonymize_sensitive_column(customer_df, 'Name')
    customer_df = anonymize_sensitive_column(customer_df, 'DateOfBirth')
    """
        # Get function to anonymize given column
        anonymization_function = self.get_corresponding_function(sensitive_column_name)
        # Get data type of the given column
        data_type = dataframe.select(sensitive_column_name).dtypes[0][1]
        # Create UDF
        anonymization_function_udf = udf(lambda z: anonymization_function(z), data_type)
        # Replace column with anonymized version
        dataframe = dataframe.withColumn(
            sensitive_column_name,
            anonymization_function_udf(col(sensitive_column_name)),
        )
        return dataframe

    def anonymize_dataframe(self, dataframe, sensitive_column_list: list):
        """
    This function takes a dataframe (DataFrame) and a list of columns to anonymize. It anonymizes the rows of the given columns and outputs the altered dataframe.
    Example usage:
    customer_df = anonymize_dataframe(customer_df, ["FirstName", "LastName", "Email", "MobileNumber"])
    """
        # Check the environment
        if "prod" in self.env.lower():
            print(
                "The data is not being anonymized since we're in the production environment"
            )
            return dataframe
        print("The data is being anonymized...")
        # Check that the given column names are in the schema
        transformed_input = [column.lower() for column in sensitive_column_list]
        dataframe_cols = [column.lower() for column in dataframe.columns]
        for column in transformed_input:
            if column not in dataframe_cols:
                print(
                    f"{column} is not part of the schema. As such, it won't be anonymized."
                )
        # Check that the columns being anonymized currently support anonymization. If not, send a message and ignore that column. Otherwise, anonymize the column.
        common = list(set(transformed_input) & set(dataframe_cols))
        for column in common:
            if column not in self.get_sensitive_field_list():
                print(
                    f"{column} does not currently support anonymization. As such, it won't be anonymized. Please expand the functions to allow for anonymization of this column."
                )
            else:
                dataframe = self.anonymize_sensitive_column(dataframe, column.title())
        return dataframe
