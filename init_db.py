from database import Database

if __name__ == "__main__":
    db = Database("service_center.db")
    db.close()
    print("OK: service_center.db создана (таблицы готовы).")