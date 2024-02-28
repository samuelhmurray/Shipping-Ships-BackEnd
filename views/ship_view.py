import sqlite3
import json


def create_ship(name, hauler_id):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        # Execute the SQL query to insert a new dock
        db_cursor.execute(
            """
            INSERT INTO Ship (name, hauler_id)
            VALUES (?, ?)
            """,
            (name, hauler_id),
        )

        # Commit the transaction
        conn.commit()

    return True


def update_ship(id, ship_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            UPDATE Ship
                SET
                    name = ?,
                    hauler_id = ?
            WHERE id = ?
            """,
            (ship_data["name"], ship_data["hauler_id"], id),
        )

        rows_affected = db_cursor.rowcount

    return True if rows_affected > 0 else False


def delete_ship(pk):
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute(
            """
        DELETE FROM Ship WHERE id = ?
        """,
            (pk,),
        )
        number_of_rows_deleted = db_cursor.rowcount

    return True if number_of_rows_deleted > 0 else False


def list_ships(url):
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        if "_expand" in url["query_params"]:
            db_cursor.execute(
                """
            SELECT
                s.id,
                s.name,
                s.hauler_id,
                h.id haulerId,
                h.name haulerName,
                h.dock_id
            FROM Ship s
            JOIN Hauler h
                ON h.id = s.hauler_id
            """
            )
        else:
            db_cursor.execute(
                """
                SELECT
                    s.id,
                    s.name,
                    s.hauler_id
                FROM Ship s
                """
            )

        query_results = db_cursor.fetchall()

        ships = []
        for row in query_results:
            ship = {"id": row["id"], "name": row["name"], "hauler_id": row["hauler_id"]}

            if "_expand" in url["query_params"]:
                ship["hauler"] = {
                    "id": row["haulerId"],
                    "name": row["haulerName"],
                    "dock_id": row["dock_id"],
                }
            ships.append(ship)
        serialized_ships = json.dumps(ships)

    return serialized_ships


def retrieve_ship(url, pk):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        if "_expand" in url["query_params"]:
            db_cursor.execute(
                """
                SELECT
                    s.id,
                    s.name,
                    s.hauler_id,
                    h.id haulerId,
                    h.name haulerName,
                    h.dock_id
                FROM Ship s
                LEFT JOIN Hauler h
                    ON h.id = s.hauler_id
                WHERE s.id = ?
                """,
                (pk,),
            )
        else:
            db_cursor.execute(
                """
                SELECT
                    s.id,
                    s.name,
                    s.hauler_id
                FROM Ship s
                WHERE s.id = ?
                """,
                (pk,),
            )

        query_result = db_cursor.fetchone()

        if query_result:
            ship = {
                "id": query_result["id"],
                "name": query_result["name"],
                "hauler_id": query_result["hauler_id"],
            }

            if "_expand" in url["query_params"]:
                ship["hauler"] = {
                    "id": query_result["haulerId"],
                    "name": query_result["haulerName"],
                    "dock_id": query_result["dock_id"],
                }

            serialized_ship = json.dumps(ship)
            return serialized_ship
        else:
            return "{}"
