#!/usr/bin/env python3

__author__      = "Ben Altieri"

import logging
import argparse
import psycopg2

# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)
logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect(database="snippets")
logging.debug("Database connection established.")

def put(name, snippet):
    """Store a snippet with an associated name."""
    logging.info("Storing snippet {!r}: {!r}".format(name, snippet))

    with connection, connection.cursor() as cursor:
        try:
            command = "insert into snippets values (%s, %s)"
            cursor.execute(command, (name, snippet))
        except psycopg2.IntegrityError as e:
            connection.rollback()
            command = "update snippets set message=%s where keyword=%s"
            cursor.execute(command, (snippet, name))

    connection.commit()
    logging.debug("Snippet stored successfully.")
    return name, snippet

def get(name):
    """Retrieve the snippet message for a given snippet keyword."""
    logging.info("Retrieving snippet for requested keyword: {!r}".format(name))

    with connection, connection.cursor() as cursor:
        cursor.execute("select message from snippets where keyword=%s", (name,))
        row = cursor.fetchone()

    if not row:
        # No snippet was found with that name.
        return "404: Snippet Not Found"
    else:
        return row

def catalog():
    logging.info("Creating a catalog view of all snippets stored in the database")

    with connection, connection.cursor() as cursor:
        cursor.execute("select * from snippets order by keyword")
        stored_snippets = cursor.fetchall()
        return stored_snippets

def search(string_to_search):
    logging.info("Searching all snippets stored in the database for substring contained in any snippet message")

    with connection, connection.cursor() as cursor:
        like_string = str("%%" + string_to_search + "%%")
        cursor.execute("select * from snippets where message like %s", (like_string,))
        results_of_search = cursor.fetchall()
        return results_of_search

def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Subparser for the put command
    logging.debug("Constructing put subparser")
    put_parser = subparsers.add_parser("put", help="Store a snippet")
    put_parser.add_argument("name", help="Name of the snippet")
    put_parser.add_argument("snippet", help="Snippet text")

    # Subparser for the get command
    logging.debug("Constructing get subparser")
    get_parser = subparsers.add_parser("get", help="Retrieve a snippet")
    get_parser.add_argument("name", help="Name of the snippet")

    # Subparser for the catalog command
    logging.debug("Constructing catalog subparser")
    catalog_parser = subparsers.add_parser("catalog", help="Create a catalog view of snippets stored in the database")

    # Subparser for the search command
    logging.debug("Constructing search subparser")
    search_parser = subparsers.add_parser("search", help="Search all snippets stored in the database for substring contained in any snippet messages")
    search_parser.add_argument("string_to_search", help="String to search within the message portion of the snippet")

    arguments = parser.parse_args()
    # Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)
    command = arguments.pop("command")

    if command == "put":
        name, snippet = put(**arguments)
        print("Attempted to store {!r} as {!r}".format(snippet, name))
    elif command == "get":
        snippet = get(**arguments)
        print("Snippet retrieval results: {!r}".format(snippet))
    elif command == "catalog":
        catalog_of_snippets = catalog()
        print("Creating a catalog view of all snippets stored in the database")
        print(catalog_of_snippets)
    elif command == "search":
        search_results = search(**arguments)
        print("Searching all snippets stored in the database for substring contained in any snippet message")
        print(search_results)

if __name__ == "__main__":
    main()
