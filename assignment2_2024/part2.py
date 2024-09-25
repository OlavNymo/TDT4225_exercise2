import datetime
import os
from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine, Unit

class ActivityTrackerProgram:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def print_query_results(self, results, headers):
        print(tabulate(results, headers=headers, tablefmt='psql'))
        print()  # Add a blank line for readability
        
    #1. Dataset counts
    def count_dataset_elements(self):
            query = """
            SELECT
                (SELECT COUNT(*) FROM User) as user_count,
                (SELECT COUNT(*) FROM Activity) as activity_count,
                (SELECT COUNT(*) FROM TrackPoint) as trackpoint_count
            """
            results = self.execute_query(query)
            headers = ['Users', 'Activities', 'Trackpoints']
            print("1. Dataset counts:")
            self.print_query_results(results, headers)
            
    #2. Average number of activities per user        
    def average_activities_per_user(self):
        query = """
        SELECT AVG(activity_count) as avg_activities
        FROM (
            SELECT user_id, COUNT(*) as activity_count
            FROM Activity
            GROUP BY user_id
        ) as user_activity_counts
        """
        results = self.execute_query(query)
        headers = ['Average Activities per User']
        print("2. Average number of activities per user:")
        self.print_query_results(results, headers)
        
    # 3. Top 20 users with the highest number of activities
    def top_20_users_by_activity_count(self):
        query = """
        SELECT user_id, COUNT(*) as activity_count
        FROM Activity
        GROUP BY user_id
        ORDER BY activity_count DESC
        LIMIT 20
        """
        results = self.execute_query(query)
        headers = ['User ID', 'Activity Count']
        print("3. Top 20 users with the highest number of activities:")
        self.print_query_results(results, headers)
        
    # 4. Users who have taken a taxi
    def users_who_took_taxi(self):
        query = """
        SELECT DISTINCT user_id
        FROM Activity
        WHERE transportation_mode = 'taxi'
        ORDER BY user_id
        """
        results = self.execute_query(query)
        headers = ['User ID']
        print("4. Users who have taken a taxi:")
        self.print_query_results(results, headers)
        
    # 5. Count of activities for each transportation mode (excluding null)
    def count_transportation_modes(self):
        query = """
        SELECT transportation_mode, COUNT(*) as activity_count
        FROM Activity
        WHERE transportation_mode IS NOT NULL
        GROUP BY transportation_mode
        ORDER BY activity_count DESC
        """
        results = self.execute_query(query)
        headers = ['Transportation Mode', 'Activity Count']
        print("5. Count of activities for each transportation mode (excluding null):")
        self.print_query_results(results, headers)
        
    # 6. a) Year with the most activities
    def year_with_most_activities(self):
        query = """
        SELECT YEAR(start_date_time) as year, COUNT(*) as activity_count
        FROM Activity
        GROUP BY year
        ORDER BY activity_count DESC
        LIMIT 1
        """
        results = self.execute_query(query)
        return results

    # 6. b) Year with the most recorded hours
    def year_with_most_recorded_hours(self):
        query = """
        SELECT 
            year, 
            ROUND(SUM(duration_hours), 2) as total_hours
        FROM (
            SELECT 
                YEAR(start_date_time) as year,
                TIMESTAMPDIFF(SECOND, start_date_time, end_date_time) / 3600.0 as duration_hours
            FROM Activity
        ) as activity_durations
        GROUP BY year
        ORDER BY total_hours DESC
        LIMIT 1
        """
        results = self.execute_query(query)
        return results

    def compare_most_activities_and_hours(self):
        # Get the year with the most activities
        activities_results = self.year_with_most_activities()
        activities_year = activities_results[0][0] if activities_results else None

        # Get the year with the most recorded hours
        hours_results = self.year_with_most_recorded_hours()
        hours_year = hours_results[0][0] if hours_results else None

        # Print results for 6a
        print("6a. Year with the most activities:")
        headers = ['Year', 'Activity Count']
        self.print_query_results(activities_results, headers)

        # Print results for 6b
        print("6b. Year with the most recorded hours:")
        headers = ['Year', 'Total Recorded Hours']
        self.print_query_results(hours_results, headers)

        # Print comparison
        if activities_year == hours_year:
            print(f"\nThe year with the most activities ({activities_year}) "
                f"is also the year with the most recorded hours.")
        else:
            print(f"\nThe year with the most activities ({activities_year}) "
                f"is different from the year with the most recorded hours ({hours_year}).")
        
    # 7. Total distance walked in 2008 by user with id=112    
    def calculate_total_walking_distance_2008_user112(self):
        query = """
        SELECT t.lat, t.lon
        FROM TrackPoint t
        JOIN Activity a ON t.activity_id = a.id
        WHERE a.user_id = '112'
          AND YEAR(a.start_date_time) = 2008
          AND a.transportation_mode = 'walk'
        ORDER BY a.id, t.id
        """
        results = self.execute_query(query)
        
        total_distance = 0
        prev_point = None
        for lat, lon in results:
            current_point = (lat, lon)
            if prev_point:
                distance = haversine(prev_point, current_point, unit=Unit.KILOMETERS)
                total_distance += distance
            prev_point = current_point

        print()
        print("7. Total distance walked in 2008 by user with id=112:")
        print(f"   {total_distance:.2f} km")
            
    # 8. Top 20 users who have gained the most altitude meters
    def top_20_users_by_altitude_gain(self):
        query = """
        WITH altitude_differences AS (
            SELECT 
                a.user_id,
                t1.activity_id,
                t1.altitude - t2.altitude AS altitude_diff_feet
            FROM TrackPoint t1
            JOIN TrackPoint t2 ON t1.activity_id = t2.activity_id AND t1.id = t2.id + 1
            JOIN Activity a ON t1.activity_id = a.id
            WHERE t1.altitude != -777 AND t2.altitude != -777
        ),
        user_altitude_gains AS (
            SELECT 
                user_id,
                SUM(IF(altitude_diff_feet > 0, altitude_diff_feet, 0)) AS total_altitude_gain_feet
            FROM altitude_differences
            GROUP BY user_id
        )
        SELECT user_id, total_altitude_gain_feet
        FROM user_altitude_gains
        ORDER BY total_altitude_gain_feet DESC
        LIMIT 20
        """
        results = self.execute_query(query)
        
        # Convert feet to meters in Python
        def feet_to_meters(feet):
            return float(feet) * 0.3048

        converted_results = [
            (user_id, round(feet_to_meters(altitude_gain), 2))
            for user_id, altitude_gain in results
        ]

        headers = ['User ID', 'Total Meters Gained']
        print()
        print("8. Top 20 users who have gained the most altitude meters:")
        self.print_query_results(converted_results, headers)
        
    # 9. Users with invalid activities and their count
    def find_users_with_invalid_activities(self):
        query = """
        WITH consecutive_points AS (
            SELECT 
                a.user_id,
                t1.activity_id,
                t1.date_time AS time1,
                t2.date_time AS time2,
                TIMESTAMPDIFF(SECOND, t1.date_time, t2.date_time) / 60 AS time_diff
            FROM TrackPoint t1
            JOIN TrackPoint t2 ON t1.activity_id = t2.activity_id AND t1.id = t2.id - 1
            JOIN Activity a ON t1.activity_id = a.id
        ),
        invalid_activities AS (
            SELECT DISTINCT user_id, activity_id
            FROM consecutive_points
            WHERE time_diff >= 5
        )
        SELECT user_id, COUNT(DISTINCT activity_id) AS invalid_activity_count
        FROM invalid_activities
        GROUP BY user_id
        ORDER BY invalid_activity_count DESC
        """
        results = self.execute_query(query)
        headers = ['User ID', 'Invalid Activity Count']
        print("\n9. Users with invalid activities and their count:")
        self.print_query_results(results, headers)

    # 10. Users who have tracked an activity in the Forbidden City of Beijing
    def find_users_in_forbidden_city(self):
        query = """
        SELECT DISTINCT a.user_id
        FROM Activity a
        JOIN TrackPoint t ON a.id = t.activity_id
        WHERE ABS(t.lat - 39.916) < 0.001 AND ABS(t.lon - 116.397) < 0.001
        ORDER BY a.user_id
        """
        results = self.execute_query(query)
        headers = ['User ID']
        print("\n10. Users who have tracked an activity in the Forbidden City of Beijing:")
        self.print_query_results(results, headers)
      
    # 11. Users with registered transportation_mode and their most used mode  
    def find_users_most_used_transportation(self):
        query = """
        SELECT user_id, transportation_mode, COUNT(*) as mode_count
        FROM Activity
        WHERE transportation_mode IS NOT NULL
        GROUP BY user_id, transportation_mode
        """
        results = self.execute_query(query)
        
        user_modes = {}
        for user_id, mode, count in results:
            if user_id not in user_modes:
                user_modes[user_id] = {}
            user_modes[user_id][mode] = count
        
        most_used_modes = []
        for user_id in sorted(user_modes.keys()):
            most_used_mode = max(user_modes[user_id], key=user_modes[user_id].get)
            most_used_modes.append((user_id, most_used_mode))
        
        headers = ['User ID', 'Most Used Transportation Mode']
        print("\n11. Users with registered transportation_mode and their most used mode:")
        self.print_query_results(most_used_modes, headers)

        
def main():
    program = None
    try:
        program = ActivityTrackerProgram()
        
        # Execute queries
        # program.count_dataset_elements()
        # program.average_activities_per_user()
        # program.top_20_users_by_activity_count()
        # program.users_who_took_taxi()
        # program.count_transportation_modes()
        # program.compare_most_activities_and_hours()
        # program.calculate_total_walking_distance_2008_user112()
        # program.top_20_users_by_altitude_gain()
        program.find_users_with_invalid_activities()
        program.find_users_in_forbidden_city()
        program.find_users_most_used_transportation()
        
    except Exception as e:
        print("An error occurred:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()