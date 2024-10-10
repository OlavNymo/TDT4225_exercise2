import datetime
import os
from DbConnector import DbConnector
from tabulate import tabulate

class ActivityTrackerProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
        user_query = """CREATE TABLE IF NOT EXISTS User (
                        id VARCHAR(255) NOT NULL PRIMARY KEY,
                        has_labels BOOLEAN)
                     """
        activity_query = """CREATE TABLE IF NOT EXISTS Activity (
                            id BIGINT NOT NULL PRIMARY KEY,
                            user_id VARCHAR(255),
                            transportation_mode VARCHAR(255),
                            start_date_time DATETIME,
                            end_date_time DATETIME,
                            FOREIGN KEY (user_id) REFERENCES User(id))
                         """
        trackpoint_query = """CREATE TABLE IF NOT EXISTS TrackPoint (
                              id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                              activity_id BIGINT,
                              lat DOUBLE,
                              lon DOUBLE,
                              altitude INT,
                              date_days DOUBLE,
                              date_time DATETIME,
                              FOREIGN KEY (activity_id) REFERENCES Activity(id))
                           """
        
        self.cursor.execute(user_query)
        self.cursor.execute(activity_query)
        self.cursor.execute(trackpoint_query)
        self.db_connection.commit()

    def insert_user_data(self, user_id, has_labels):
        query = "INSERT INTO User (id, has_labels) VALUES (%s, %s)"
        self.cursor.execute(query, (user_id, has_labels))
        self.db_connection.commit()

    def insert_activity_data(self, activity_id, user_id, activity_data):
        query = """INSERT INTO Activity 
                    (id, user_id, transportation_mode, start_date_time, end_date_time) 
                    VALUES (%s, %s, %s, %s, %s)"""
        self.cursor.execute(query, (
            int(activity_id),
            user_id,
            None,  # transportation_mode is not provided in the file
            activity_data['start_date_time'],
            activity_data['end_date_time']
        ))
        self.db_connection.commit()
        
    def insert_trackpoints_batch(self, trackpoints):
        query = """INSERT INTO TrackPoint 
                   (activity_id, lat, lon, altitude, date_days, date_time) 
                   VALUES (%s, %s, %s, %s, %s, %s)"""
        self.cursor.executemany(query, trackpoints)
        self.db_connection.commit()

    def insert_trackpoint_data(self, activity_id, lat, lon, altitude, date_days, date_time):
        query = """INSERT INTO TrackPoint 
                   (activity_id, lat, lon, altitude, date_days, date_time) 
                   VALUES (%s, %s, %s, %s, %s, %s)"""
        self.cursor.execute(query, (activity_id, lat, lon, altitude, date_days, date_time))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(f"Data from table {table_name}, tabulated:")
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print(f"Dropping table {table_name}...")
        query = f"DROP TABLE IF EXISTS {table_name}"
        self.cursor.execute(query)
        self.db_connection.commit()

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def populate_user_table(self, dataset_path):
        labeled_ids_path = os.path.join(dataset_path, 'dataset', 'labeled_ids.txt')
    
        # Read labeled user IDs
        with open(labeled_ids_path, 'r') as f:
            labeled_ids = set(f.read().splitlines())
        
        # Track processed users
        processed_users = set()
        
        # Walk through the directory structure
        data_path = os.path.join(dataset_path, 'dataset', 'Data')
        for root, dirs, files in os.walk(data_path):
            for dir in dirs:
                user_id = dir
                if user_id == "Trajectory": # Skip the Trajectory "user"
                    continue
                if user_id in processed_users:
                    continue  # Skip already processed users

                has_labels = user_id in labeled_ids

                # Insert new user data
                self.insert_user_data(user_id, has_labels)

                # Mark user as processed
                processed_users.add(user_id)
        
        print("User table populated successfully.")
        
    def populate_activity_table(self, dataset_path):
        data_path = os.path.join(dataset_path, 'dataset', 'Data')
        for root, dirs, files in os.walk(data_path):
            if 'Trajectory' in root:
                user_id = os.path.basename(os.path.dirname(root))
                for file in files:
                    if file.endswith('.plt'):
                        activity_id_str = f"{user_id}{os.path.splitext(file)[0]}"
                        
                        activity_id = int(activity_id_str)
                        
                        file_path = os.path.join(root, file)
                        
                        activity_data = self.process_activity_file(file_path)
                        
                        if activity_data:
                            self.insert_activity_data(activity_id, user_id, activity_data)
                        else:
                            print(f"Skipped activity {activity_id} for user {user_id} due to too many trackpoints or missing data.")
        
        print("Activity table populated successfully.")
        
    def populate_trackpoint_table(self, dataset_path):
        data_path = os.path.join(dataset_path, 'dataset', 'Data')
        batch_size = 1000
        trackpoints_batch = []
        
        for root, dirs, files in os.walk(data_path):
            if 'Trajectory' in root:
                user_id = os.path.basename(os.path.dirname(root))
                for file in files:
                    if file.endswith('.plt'):
                        activity_id_str = f"{user_id}{os.path.splitext(file)[0]}"
                        try:
                            activity_id = int(activity_id_str)
                        except ValueError:
                            print(f"Invalid activity_id generated: {activity_id_str}")
                            continue
                        file_path = os.path.join(root, file)
                        
                        trackpoints = self.process_trackpoints(file_path, activity_id)
                        
                        if trackpoints:
                            trackpoints_batch.extend(trackpoints)
                            
                            if len(trackpoints_batch) >= batch_size:
                                self.insert_trackpoints_batch(trackpoints_batch)
                                trackpoints_batch = []
        
        # Insert any remaining trackpoints
        if trackpoints_batch:
            self.insert_trackpoints_batch(trackpoints_batch)
        
        print("TrackPoint table populated successfully.")
        
    def process_activity_file(self, file_path):
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()[6:]  # Skip first 6 lines
                
                if len(lines) > 2500:
                    print(f"Skipping file {file_path} due to too many trackpoints ({len(lines)}).")
                    return None  # Skip activities with more than 2500 trackpoints
                
                start_time = None
                end_time = None
                
                for line_num, line in enumerate(lines, start=7):  # Start counting from 7 to account for skipped lines
                    try:
                        parts = line.strip().split(',')
                        if len(parts) >= 7:
                            # Ensure we have enough parts before accessing them
                            lat, lon = parts[0], parts[1]
                            altitude = parts[3]
                            date = parts[5]
                            time = parts[6]
                            
                            # Parse datetime
                            date_string = f"{date} {time}"
                            try:
                                current_time = datetime.datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
                            except ValueError as e:
                                print(f"Error parsing date {date_string} in file {file_path}, line {line_num}: {e}. Skipping this line.")
                                continue
                            
                            if start_time is None:
                                start_time = current_time
                            end_time = current_time
                        else:
                            print(f"Warning: Line {line_num} in {file_path} has fewer than 7 columns. Skipping this line.")
                    except Exception as e:
                        print(f"Error processing line {line_num} in file {file_path}: {e}. Line content: {line.strip()}")
                        continue

                if start_time and end_time:
                    return {
                        'start_date_time': start_time,
                        'end_date_time': end_time
                    }
                else:
                    print(f"Missing start or end time in file {file_path}.")
                return None
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            return None
        
    def process_trackpoints(self, file_path, activity_id):
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()[6:]  # Skip first 6 lines
                
                if len(lines) > 2500:
                    return None  # Skip activities with more than 2500 trackpoints
                
                trackpoints = []
                
                for line in lines:
                    parts = line.strip().split(',')
                    if len(parts) >= 7:
                        lat, lon = float(parts[0]), float(parts[1])
                        altitude = int(float(parts[3]))
                        date_days = float(parts[4])
                        date_string = f"{parts[5]} {parts[6]}"
                        date_time = datetime.datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
                        
                        trackpoints.append((activity_id, lat, lon, altitude, date_days, date_time))

                return trackpoints
        except Exception as e:
            print(f"Error processing trackpoints in file {file_path}: {e}")
            return None
    
        
    def update_transportation_modes(self, dataset_path):
        labels = self.read_labels(dataset_path)
        users_with_labels = self.get_users_with_labels()
        
        for user_id in users_with_labels:
            if user_id not in labels:
                print(f"Error: User {user_id} has has_labels set to true, but no transportation labels were found.")
                continue
            
            activities = self.get_user_activities(user_id)
            labels_found = False
            
            for activity in activities:
                transportation_mode = self.find_matching_label(user_id, activity, labels)
                if transportation_mode:
                    self.update_activity_transportation_mode(activity['id'], transportation_mode)
                    labels_found = True
            
            if not labels_found:
                print(f"Error: User {user_id} has has_labels set to true, but no matching transportation labels were found for any activities.")

        print("Transportation modes updated successfully.")

    def get_users_with_labels(self):
        query = "SELECT id FROM User WHERE has_labels = TRUE"
        self.cursor.execute(query)
        return [row[0] for row in self.cursor.fetchall()]

    def get_user_activities(self, user_id):
        query = """SELECT id, start_date_time, end_date_time 
                   FROM Activity 
                   WHERE user_id = %s"""
        self.cursor.execute(query, (user_id,))
        return [{'id': row[0], 'start_date_time': row[1], 'end_date_time': row[2]} 
                for row in self.cursor.fetchall()]

    def update_activity_transportation_mode(self, activity_id, transportation_mode):
        query = """UPDATE Activity 
                   SET transportation_mode = %s 
                   WHERE id = %s"""
        self.cursor.execute(query, (transportation_mode, activity_id))
        self.db_connection.commit()

    def read_labels(self, dataset_path):
        labels = {}
        labeled_ids_path = os.path.join(dataset_path, 'dataset', 'labeled_ids.txt')
        
        with open(labeled_ids_path, 'r') as f:
            labeled_ids = set(f.read().splitlines())
        
        for user_id in labeled_ids:
            labels_file = os.path.join(dataset_path, 'dataset', 'Data', user_id, 'labels.txt')
            if os.path.exists(labels_file):
                with open(labels_file, 'r') as f:
                    user_labels = []
                    for line in f.readlines()[1:]:  # Skip header
                        parts = line.strip().split('\t')
                        if len(parts) == 3:
                            start_time = datetime.datetime.strptime(parts[0], "%Y/%m/%d %H:%M:%S")
                            end_time = datetime.datetime.strptime(parts[1], "%Y/%m/%d %H:%M:%S")
                            mode = parts[2]
                            user_labels.append((start_time, end_time, mode))
                    labels[user_id] = user_labels
        
        return labels

    def find_matching_label(self, user_id, activity, labels):
        if user_id not in labels:
            return None
        
        for start_time, end_time, mode in labels[user_id]:
            if (start_time == activity['start_date_time'] and 
                end_time == activity['end_date_time']):
                return mode
        
        return None
    
    def verify_transportation_modes(self, dataset_path):
        labels = self.read_labels(dataset_path)
        users_with_labels = self.get_users_with_labels()
        
        total_activities = 0
        correct_activities = 0
        inconsistent_activities = []

        for user_id in users_with_labels:
            if user_id not in labels:
                print(f"Error: User {user_id} has has_labels set to true, but no labels file was found.")
                continue
            
            activities = self.get_user_activities_with_transportation(user_id)
            
            for activity in activities:
                total_activities += 1
                label_mode = self.find_matching_label(user_id, activity, labels)
                
                if label_mode is not None:
                    if label_mode == activity['transportation_mode']:
                        correct_activities += 1
                    else:
                        inconsistent_activities.append({
                            'user_id': user_id,
                            'activity_id': activity['id'],
                            'db_mode': activity['transportation_mode'],
                            'label_mode': label_mode
                        })

        print(f"Verification complete. {correct_activities} out of {total_activities} activities with labels are correct.")
        
        if inconsistent_activities:
            print("\nInconsistent activities found:")
            for activity in inconsistent_activities:
                print(f"User: {activity['user_id']}, Activity: {activity['activity_id']}, "
                      f"DB Mode: {activity['db_mode']}, Label Mode: {activity['label_mode']}")
        else:
            print("No inconsistencies found.")

    def get_user_activities_with_transportation(self, user_id):
        query = """SELECT id, start_date_time, end_date_time, transportation_mode 
                   FROM Activity 
                   WHERE user_id = %s"""
        self.cursor.execute(query, (user_id,))
        return [{'id': row[0], 'start_date_time': row[1], 'end_date_time': row[2], 'transportation_mode': row[3]} 
                for row in self.cursor.fetchall()]

        
def main():
    program = None
    try:
        program = ActivityTrackerProgram()
        program.drop_table("TrackPoint")
        program.drop_table("Activity")
        program.drop_table("User")
        program.create_tables()
        
        dataset_path = 'dataset' 
        program.populate_user_table(dataset_path)
        program.populate_activity_table(dataset_path)
        program.populate_trackpoint_table(dataset_path)
        
        program.fetch_data("User")
        program.fetch_data("Activity")
        program.show_tables()
        
        program.update_transportation_modes(dataset_path)
        program.verify_transportation_modes(dataset_path)
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
    