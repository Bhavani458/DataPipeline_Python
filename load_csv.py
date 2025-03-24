import mysql.connector #library to connect with mysql
import csv #to work with csv files

#function to delete duplicate record
def delete_old_records(conn,csv_file_path):
    #cursor to run sql queries
    cur = conn.cursor()

    #delete data if file path already exists 
    delete_query = "DELETE FROM earthquake WHERE file_path =%s"

    cur.execute(delete_query,(csv_file_path,))

    #commit to connection
    conn.commit()
    #close cursor
    cur.close() 

#function to load csv data
def load_csv_mysql(conn,csv_file_path):
    cur = conn.cursor()
    
    #open already extracted csv file as read
    with open(csv_file_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header row
        data = [tuple(val.strip() if val else None for val in row) for row in reader]  # Handle empty values

        #insert query to work with csv data
        sql = f"INSERT INTO earthquake ({', '.join(header)}) VALUES ({', '.join(['%s'] * len(header))})"

        #execute using cursor
        cur.executemany(sql, data)

    conn.commit()
    cur.close()

    #close connection
    conn.close()
    print("Data Loaded successfully!")

def main():
    #database parameters
    db_params = {

        'database': 'DataPipeline_Loading',
        'user':'root',
        'password':'Hyderabad@2023',
        'host':'127.0.0.1'
    }
    #connect to mysql database
    conn = mysql.connector.connect(**db_params)

    csv_file_path = "/Users/bhavanipriya/Documents/DataPipeline_Python/data/earthquake_2025_03_21.csv"

    #try-except block
    try:
        delete_old_records(conn,csv_file_path)

        load_csv_mysql(conn, csv_file_path)
    
    except Exception as e:
        print(f"An error occured: {str(e)}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()



