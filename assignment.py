"""
Hello Python developer

Good luck with this coding exercise for becoming a Python developer at Channable!

# First, some context:
Channable is an online tool that imports products from its users' eCommerceplatforms (e.g. WooCommerce) every day,
processes those products, and sends updates for those products to marketing channels (e.g. Amazon or eBay). 
Technically speaking, Channable sends ​create​, ​update, and ​delete​ operations for these products. Every day when 
Channable imports the products from the eCommerce platform, Channable needs to decide which operation it needs 
to send to the marketing channel:

  - Create: ​the product wasn’t imported from the eCommerce system yesterday, 
    but it was imported today. This means we have to send a ​create operation 
    ​to the eCommerce platform

  - Update:​ the product was imported yesterday and is also imported today, 
    however, one of the values for the products has changed (e.g. the price of
    the product). This means we have to send an ​update operation​ to the 
    marketing channel

  - Delete: ​the product was imported yesterday, but was not imported today. 
    This means we have to send a ​delete operation ​to the marketing channel


# The assignment:
In this assignment you are asked to make a basic implementation of the logic described above. You should have
received two CSV files to resemble the data that is imported from the eCommerce system:

  - product_inventory_before.csv​ (resembles the product data that was imported yesterday)
  - product_inventory_after.csv ​(resembles the product data that was imported today)

For this assignment you need to build a program that compares the product data between the `before CSV` and 
the `after CSV`. The `​id​` column can be assumed to be a unique identifier for the products in both CSVs. The 
output should give the create, update, and delete operations that should be sent to the marketing channel.


# Requirements:
  - The program should be a single ​.py ​​file (no compressed files, such as .zip, .rar, etc.)
  - The program should be written in ​python 3.7​, using only python’s ​built-in libraries.
  - You have to implement the `ProductDiffer` class below and specifically its entry point called `main`. 
  - The `ProductStreamProcessor` should not be changed.
  - The output of main is a sequence of operations in the form of triples that contain:
        1. the operation type
        2. the product id
        3. either a dictionary with the complete product data where the keys are the column names
           from the CSV files or a `None`


# Note:
The assignment is consciously kept a bit basic to make sure you don’t have to spend hours and hours on this 
assignment. However, even though the assignment itself is quite basic, we would like you to show us how you 
would structure your code to make it easily readable, so others can trust it works as intended.
"""
import abc
import csv
from enum import Enum, auto
from typing import Tuple, Optional, Dict, Iterator, Any, TextIO, List, Union


class Operation(Enum):
    CREATE = auto()
    UPDATE = auto()
    DELETE = auto()


class ProductStreamProcessor(metaclass=abc.ABCMeta):
    # Note the methods of this ProductStreamProcessor class should not be adjusted
    # as this is a hypothetical base class shared with other programs. 

    def __init__(self, path_to_before_csv: str, path_to_after_csv: str):
        self.path_to_before_csv = path_to_before_csv
        self.path_to_after_csv = path_to_after_csv

    @abc.abstractmethod
    def main(self) -> Iterator[Tuple[Operation, str, Optional[Dict[str, Any]]]]:
        """
        Creates a stream of operations based for products in the form of tuples
        where the first element is the operation, the second element is the id
        for the product, and the third is a dictionary with all data for a
        product. The latter is None for DELETE operations.
        """
        ...


class ProductDiffer(ProductStreamProcessor):
    """
    Implement this class to create a simple product differ.
    """
    @staticmethod
    def convert_data_from_csv(csv_products, headers) -> List[Dict[str, Dict[str, Any]]]:
        """
        Converts the data from the CSV file to a list of dictionaries.

        :param csv_products: The CSV file reader
        :param headers: The headers of the CSV file
        :return: A list of dictionaries with the data from the CSV file
        """
        products: List[Dict[str, Dict[str, Any]]] = []
        headers_cnt: int = len(headers)
        for product in csv_products:
            product_data = dict(
                data={},
                id=product[0],
            )
            for idx in range(headers_cnt):
                product_data['data'][headers[idx]] = product[idx]
            products.append(product_data)
        return products

    @staticmethod
    def find_suitable_product(products: List[Dict[str, Dict[str, Any]]], match_id: int):
        """
        Finds a product in the list of products that matches the id.

        :param products: list of products
        :param match_id: id to match
        :return: product that matches the id
        """
        for product in products:
            if product['id'] == match_id:
                return product
        return None

    @staticmethod
    def init_file_readers(before_csv_file: TextIO, after_csv_file: TextIO) -> Tuple[csv.reader, csv.reader]:
        """
        Initializes the csv.reader objects for the before and after csv files.

        :param before_csv_file: the before csv file
        :param after_csv_file: the after csv file
        :return: the before and after csv.reader objects
        """
        before_csv_reader: csv.reader = csv.reader(before_csv_file)
        after_csv_reader: csv.reader = csv.reader(after_csv_file)
        return before_csv_reader, after_csv_reader

    def prepare_data(
        self,
        before_csv_reader: csv.reader,
        after_csv_reader: csv.reader,
    ) -> Tuple[List[Dict[str, Union[Dict[str, Any], int]]], List[Dict[str, Union[Dict[str, Any], int]]]]:
        """
        Prepares the data for the differ.

        :param before_csv_reader: the before csv reader
        :param after_csv_reader: the after csv reader
        :return: the before and after data
        """
        next(before_csv_reader)  # We need to skip the headers (first row)
        file_headers: List[str] = next(after_csv_reader)  # We need to skip the headers (first row)
        before_csv_products: List[Dict[str, Dict[str, Any]]] = self.convert_data_from_csv(
            before_csv_reader,
            file_headers,
        )
        after_csv_products: List[Dict[str, Dict[str, Any]]] = self.convert_data_from_csv(
            after_csv_reader,
            file_headers,
        )
        return before_csv_products, after_csv_products

    def main(self) -> Iterator[Tuple[Operation, str, Optional[Dict[str, Any]]]]:
        """
        Creates a stream of operations based for products in the form of tuples
        where the first element is the operation, the second element is the id
        for the product, and the third is a dictionary with all data for a
        product. The latter is None for DELETE operations.

        :return: a stream of operations
        """
        with open(self.path_to_before_csv, 'r') as before_csv_file, open(self.path_to_after_csv, 'r') as after_csv_file:
            before_csv_reader, after_csv_reader = self.init_file_readers(before_csv_file, after_csv_file)
            before_products, after_products = self.prepare_data(before_csv_reader, after_csv_reader)
            for before_product in before_products:
                suitable_product = self.find_suitable_product(after_products, before_product['id'])
                if suitable_product is not None:
                    yield Operation.UPDATE, before_product['id'], suitable_product['data']
                else:
                    yield Operation.DELETE, before_product['id'], None

            for after_product in after_products:
                suitable_product = self.find_suitable_product(before_products, after_product['id'])
                if suitable_product is None:
                    yield Operation.CREATE, after_product['id'], after_product['data']
