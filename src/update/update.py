def update_date(pg_engine):
    new_latest_update = "SELECT MAX(updated_at) FROM threads"
    pg_engine.execute(
            f"TRUNCATE latest_update_date; INSERT INTO latest_update_date ({new_latest_update});")

    print(f"Updated latest_update_date successfully!")