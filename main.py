from app.server import app
import functions.data_treatment as dt

def main():
    dt.create_sqlite()
    app.run()
    

if __name__ == "__main__":
    main()