from app.server import app
import functions.sql_management as sqlm

def main():
    sqlm.create_sqlite()
    app.run()
    

if __name__ == "__main__":
    main()