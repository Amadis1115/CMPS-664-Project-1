# Lets start with the libraries I need for this project
import pandas as pd
import itertools
import sqlite3
import os


# I need to create a function that defines each table before I proceed
def get_relation_info():
    # Now I will store all of the Part A tables
    relations = {}

    # Here I am storing the Passenger table information
    relations["1"] = {
        "name": "Passenger",
        "attrs": ["passenger_id", "firstname", "lastname", "age", "address"],
        "pk": {"passenger_id"},
        "fds": "passenger_id->firstname+lastname+age+address"
    }

    # Here I am storing the Class table information
    relations["2"] = {
        "name": "Class",
        "attrs": ["class_id", "class_name"],
        "pk": {"class_id"},
        "fds": "class_id->class_name"
    }

    # Here I am storing the Airports table information
    relations["3"] = {
        "name": "Airports",
        "attrs": ["iata_code"],
        "pk": {"iata_code"},
        "fds": ""
    }

    # Here I am storing the Flights table information
    relations["4"] = {
        "name": "Flights",
        "attrs": ["flight_id", "source_iata", "dest_iata", "travelDATE"],
        "pk": {"flight_id"},
        "fds": "flight_id->source_iata+dest_iata+travelDATE"
    }

    # Here I am storing the Reservation table information
    relations["5"] = {
        "name": "Reservation",
        "attrs": ["reservation_id", "booking_time", "passenger_id", "flight_id", "requested_class_id", "n_pass"],
        "pk": {"reservation_id"},
        "fds": "reservation_id->booking_time+passenger_id+flight_id+requested_class_id+n_pass"
    }

    # Here I am storing the Seats table information
    relations["6"] = {
        "name": "Seats",
        "attrs": ["seat_id", "flight_id", "seat_number", "class_id", "is_available"],
        "pk": {"seat_id"},
        "fds": "seat_id->flight_id+seat_number+class_id+is_available"
    }

    # Here I am storing the CheckIn table information
    relations["7"] = {
        "name": "CheckIn",
        "attrs": ["checkin_id", "reservation_id", "checkin_time"],
        "pk": {"checkin_id"},
        "fds": "checkin_id->reservation_id+checkin_time"
    }

    # Here I am storing the Reservation_Seat table information
    relations["8"] = {
        "name": "Reservation_Seat",
        "attrs": ["reservation_id", "seat_id"],
        "pk": {"reservation_id", "seat_id"},
        "fds": ""
    }

    # Now I will print the menu for the user
    print("Please Select a table from Part A")
    print("1) Passenger")
    print("2) Class")
    print("3) Airports")
    print("4) Flights")
    print("5) Reservation")
    print("6) Seats")
    print("7) CheckIn")
    print("8) Reservation_Seat")

    # Now I will ask the user for their choice
    choice = input("Enter choice: ").strip()

    # If the choice exists in the relations dictionary, return that table
    if choice in relations:
        return relations[choice]

    # If the user enters an invalid option, print an error
    print("Invalid choice.")
    return None


# I will work here on the CSV loading/input
def load_csv():
    # Now I will ask the user for the CSV path
    path = input("CSV path: ").strip()

    # If the file path does not exist, print an error
    if not os.path.exists(path):
        print("File not found.")
        return None

    # Now I will read the CSV into a dataframe
    df = pd.read_csv(path)

    # Here I will print a quick summary of the CSV file
    print("CSV SUMMARY")
    print("Rows:", df.shape[0], "Columns:", df.shape[1])
    print(df.head())
    print(df.dtypes)

    return df


# I will work on my Functional Dependency parsing function here
def parse_fds(fd_string):
    # This list will store the functional dependencies
    fds = []

    # If no FDs are entered, just return an empty list
    if fd_string.strip() == "":
        return fds

    # Now I will split the FD string by commas
    for fd in fd_string.split(","):
        fd = fd.strip()

        # Only continue if the FD contains ->
        if "->" in fd:
            left, right = fd.split("->")

            # Now I will build the left side set
            left_set = set()
            for item in left.split("+"):
                item = item.strip()
                if item != "":
                    left_set.add(item)

            # Now I will build the right side set
            right_set = set()
            for item in right.split("+"):
                item = item.strip()
                if item != "":
                    right_set.add(item)

            # Now I will store the FD as a tuple
            fds.append((left_set, right_set))

    return fds


# Now I will work on the closure function
def closure(attrs, fds):
    # Start the closure with the original set of attributes
    result = set(attrs)
    changed = True

    # Keep looping until no new attributes are added
    while changed:
        changed = False
        for left, right in fds:
            # If the left side is already in the closure, add the right side
            if left.issubset(result) and not right.issubset(result):
                for attr in right:
                    if attr not in result:
                        result.add(attr)
                        changed = True

    return result


# This function checks whether a set of attributes is a superkey
def is_superkey(attrs, all_attrs, fds):
    return closure(attrs, fds) == set(all_attrs)


# Now I will work on candidate keys
def candidate_keys(all_attrs, fds):
    # This list will store all candidate keys
    keys = []

    # Convert the attributes to a list so I can generate combinations
    attrs = list(all_attrs)

    # Now I will test all possible combinations of attributes
    for r in range(1, len(attrs) + 1):
        for combo in itertools.combinations(attrs, r):
            combo_set = set(combo)

            # If the combination is a superkey, check if it is minimal
            if is_superkey(combo_set, all_attrs, fds):
                minimal = True

                for other in keys:
                    if other < combo_set:
                        minimal = False
                        break

                # If it is minimal, it is a candidate key
                if minimal:
                    keys.append(combo_set)

    return keys


# Here I will focus on my Normalization checks
def check_1nf(df, attrs):
    # Now I will make sure all attributes exist in the CSV
    for attr in attrs:
        if attr not in df.columns:
            return False

        # Now I will check that the values are atomic
        for value in df[attr]:
            if isinstance(value, (list, dict, set, tuple)):
                return False

    return True


# This function will find partial dependencies for 2NF
def partial_deps(pk, fds):
    # This list will store all partial dependency violations
    violations = []

    # If the primary key has only one attribute, then there cannot be partial dependencies
    if len(pk) <= 1:
        return violations

    # Now I will loop through each FD
    for left, right in fds:
        if left < pk:
            non_key = set()

            # Now I will collect only non-key attributes on the right side
            for attr in right:
                if attr not in pk:
                    non_key.add(attr)

            # If there are non-key attributes, store the violation
            if len(non_key) > 0:
                violations.append((left, non_key))

    return violations


# This function will find transitive dependencies for 3NF
def transitive_deps(attrs, fds, keys):
    # First I will find all prime attributes
    primes = set()

    for key in keys:
        for attr in key:
            primes.add(attr)

    # This list will store the transitive dependency violations
    violations = []

    # Now I will check each FD
    for left, right in fds:
        for attr in right:
            if attr not in left:
                if not is_superkey(left, attrs, fds) and attr not in primes:
                    violations.append((left, {attr}))

    return violations


# This function will find BCNF violations
def bcnf_violations(attrs, fds):
    # This list will store BCNF violations
    violations = []

    # Now I will loop through each FD
    for left, right in fds:
        non_trivial = set()

        # Ignore trivial dependencies
        for attr in right:
            if attr not in left:
                non_trivial.add(attr)

        # In BCNF, the left side must be a superkey
        if len(non_trivial) > 0:
            if not is_superkey(left, attrs, fds):
                violations.append((left, non_trivial))

    return violations


# Now I will work on the Simple Decomposition
def decompose(relation, attrs, pk, fds):
    # This list will store the decomposed relations
    rels = []

    # First I will store the main relation
    main_relation = {}
    main_relation["name"] = relation
    main_relation["attrs"] = attrs
    main_relation["pk"] = list(pk)
    rels.append(main_relation)

    # Now I will collect all types of violations
    p = partial_deps(pk, fds)
    t = transitive_deps(attrs, fds, candidate_keys(attrs, fds))
    b = bcnf_violations(attrs, fds)

    idx = 1

    # Now I will combine all violations into one list
    all_violations = []
    for item in p:
        all_violations.append(item)
    for item in t:
        all_violations.append(item)
    for item in b:
        all_violations.append(item)

    # Now I will create a new relation for each violation
    for left, right in all_violations:
        new_relation = {}
        new_relation["name"] = relation + "_D" + str(idx)

        new_attrs = []
        for attr in attrs:
            if attr in left or attr in right:
                new_attrs.append(attr)

        new_relation["attrs"] = new_attrs
        new_relation["pk"] = list(left)
        rels.append(new_relation)

        idx += 1

    return rels


# Now I will work on the SQL Generation
def write_sql(rels):
    # Open the SQL output file
    with open("output.sql", "w") as f:
        for r in rels:
            sql = "CREATE TABLE " + r["name"] + " ("

            # Now I will add each attribute as TEXT
            for i in range(len(r["attrs"])):
                sql += r["attrs"][i] + " TEXT"
                if i < len(r["attrs"]) - 1:
                    sql += ", "

            # Now I will add the primary key if it exists
            if len(r["pk"]) > 0:
                sql += ", PRIMARY KEY (" + ", ".join(r["pk"]) + ")"

            sql += ");"
            f.write(sql)

    print("SQL file created.")


# Now I will work on the Database Creation and Population
def create_db(rels):
    # Create the SQLite connection
    conn = sqlite3.connect("project.db")
    cur = conn.cursor()

    # Now I will create each decomposed relation as a table
    for r in rels:
        sql = "CREATE TABLE IF NOT EXISTS " + r["name"] + " ("

        for i in range(len(r["attrs"])):
            sql += r["attrs"][i] + " TEXT"
            if i < len(r["attrs"]) - 1:
                sql += ", "

        if len(r["pk"]) > 0:
            sql += ", PRIMARY KEY (" + ", ".join(r["pk"]) + ")"

        sql += ");"
        cur.execute(sql)

    conn.commit()
    return conn


# Now I will populate the tables with data from the CSV
def populate(conn, df, rels):
    cur = conn.cursor()

    # Loop through each relation
    for r in rels:
        available = []

        # Only keep the attributes that exist in the dataframe
        for attr in r["attrs"]:
            if attr in df.columns:
                available.append(attr)

        if len(available) == 0:
            continue

        # Remove duplicate rows before inserting
        sub = df[available].drop_duplicates()

        for index, row in sub.iterrows():
            vals = []

            # Now I will build the values list
            for attr in available:
                if pd.notna(row[attr]):
                    vals.append(str(row[attr]))
                else:
                    vals.append(None)

            # Build the placeholder string
            placeholders = ""
            for i in range(len(vals)):
                placeholders += "?"
                if i < len(vals) - 1:
                    placeholders += ", "

            sql = "INSERT INTO " + r["name"] + " (" + ", ".join(available) + ") VALUES (" + placeholders + ")"
            cur.execute(sql, vals)

    conn.commit()
    print("Tables populated.")


# Now I work on the Query Interface for the tool
def query_interface(conn):
    cur = conn.cursor()

    # Keep showing the menu until the user exits
    while True:
        print("1) Insert  2) Update  3) Delete  4) SQL  5) Exit")
        c = input("Choice: ").strip()

        # Insert option
        if c == "1":
            t = input("Table: ")
            cols = input("Columns: ").split(",")
            vals = input("Values: ").split(",")

            clean_cols = []
            for col in cols:
                clean_cols.append(col.strip())

            clean_vals = []
            for val in vals:
                clean_vals.append(val.strip())

            placeholders = ""
            for i in range(len(clean_vals)):
                placeholders += "?"
                if i < len(clean_vals) - 1:
                    placeholders += ", "

            sql = "INSERT INTO " + t + " (" + ", ".join(clean_cols) + ") VALUES (" + placeholders + ")"
            cur.execute(sql, clean_vals)
            conn.commit()
            print("Inserted.")

        # Update option
        elif c == "2":
            t = input("Table: ")
            setc = input("SET clause: ")
            w = input("WHERE (optional): ")

            sql = "UPDATE " + t + " SET " + setc
            if w != "":
                sql += " WHERE " + w

            cur.execute(sql)
            conn.commit()
            print("Updated.")

        # Delete option
        elif c == "3":
            t = input("Table: ")
            w = input("WHERE (optional): ")

            sql = "DELETE FROM " + t
            if w != "":
                sql += " WHERE " + w

            cur.execute(sql)
            conn.commit()
            print("Deleted.")

        # Custom SQL option
        elif c == "4":
            sql = input("SQL: ")

            try:
                cur.execute(sql)

                if sql.strip().lower().startswith("select"):
                    rows = cur.fetchall()
                    for row in rows:
                        print(row)
                else:
                    conn.commit()
                    print("Query executed successfully.")

            except Exception as e:
                print("Error:", e)

        # Exit option
        elif c == "5":
            break


# Now I will work on the main function
def main():
    import os
    print("Current working directory:", os.getcwd())
    # First I will ask the user which Part A table they want to analyze
    relation_info = get_relation_info()
    if relation_info is None:
        return

    # Now I will load the CSV for that table
    df = load_csv()
    if df is None:
        return

    # Now I will assign the relation details automatically
    rel = relation_info["name"]
    attrs = relation_info["attrs"]
    pk = relation_info["pk"]
    fds = parse_fds(relation_info["fds"])

    # Now I will print the analysis summary
    print("Analysis Below!")
    print("Relation:", rel)
    print("Attributes:", attrs)
    print("Primary Key:", pk)
    print("1NF:", "Yes" if check_1nf(df, attrs) else "No")

    # Now I will compute the candidate keys and violations
    keys = candidate_keys(attrs, fds)
    print("Candidate Keys:", keys)
    print("Partial deps:", partial_deps(pk, fds))
    print("Transitive deps:", transitive_deps(attrs, fds, keys))
    print("BCNF violations:", bcnf_violations(attrs, fds))

    # Now I will decompose the relation
    rels = decompose(rel, attrs, pk, fds)

    print("Decomposed Relations:")
    for r in rels:
        print(r)

    # Ask the user if they want to generate SQL
    if input("Generate SQL? ").lower() == "yes":
        write_sql(rels)

    # Ask the user if they want to create the database and populate it
    if input("Create DB + populate? ").lower() == "yes":
        conn = create_db(rels)
        populate(conn, df, rels)

        # Ask the user if they want to start the query interface
        if input("Start query interface? ").lower() == "yes":
            query_interface(conn)

        conn.close()

    print("Done.")


if __name__ == "__main__":
    main()