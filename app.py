from flask import Flask, jsonify, request
from flask_marshmallow import Marshmallow
from marshmallow import fields, ValidationError
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)
ma = Marshmallow(app)

class MemberSchema(ma.Schema):
    name = fields.String(required=True)
    age = fields.Integer(required=True)
    
    class Meta:
        fields = ('name','age','id')

customer_schema = MemberSchema()
customers_schema = MemberSchema(many=True)

class SessionSchema(ma.Schema):
    member_id = fields.Integer(required=True)
    session_date = fields.String(required=True)
    session_time = fields.String(required=True)
    activity = fields.String(required=True)

    class Meta:
        fields = ('session_id','member_id','session_date','session_time','activity',)

session_schema = SessionSchema()
sessions_schema = SessionSchema(many=True)

def get_db_connection():
    '''Connect to the MySQL database and return the connection object'''
    db_name = 'fitness_center_db'
    user = 'root'
    password = 'MySQL1sLif3!'
    host = 'localhost'

    try:
        conn = mysql.connector.connect(
            database = db_name,
            user = user,
            password = password,
            host = host,
            use_pure = True
        )
        print("\nSuccessfully connected to MySQL database.")
        return conn
    
    except Error as e:
        print(f"Error: {e}")
        return None
    
@app.route('/')
def home():
    return "Welcome to the Fitness Center Database!"

# Task 2

@app.route('/members', methods=['GET'])
def get_members():
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error": "Database connection failed."}), 500
        cursor = conn.cursor(dictionary=True)

        query = 'SELECT * FROM members'
        cursor.execute(query)

        members = cursor.fetchall()
        return customers_schema.jsonify(members)

    except Error as e:
        print(f"Error: {e}")
        return jsonify({"Error": "Internal Server Error"}), 500

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/members', methods=['POST'])
def add_members():
    try:
        user_data = customer_schema.load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error": "Database connection failed."}), 500
        cursor = conn.cursor()

        new_member = (user_data['name'], user_data['age'])

        query = 'INSERT INTO members (name, age) VALUES(%s,%s)'
        cursor.execute(query,new_member)
        conn.commit()
        return jsonify({"Message": "New member successfully added"}), 201

    except Error as e:
        print(f"Error: {e}")
        return jsonify({"Error": "Internal Server Error"}), 500

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/members/<int:id>', methods=['PUT'])
def update_members(id):
    try:
        user_data = customer_schema.load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error": "Database connection failed."}), 500
        cursor = conn.cursor()

        updated_member = (user_data['name'], user_data['age'], id)

        query = 'UPDATE members SET name = %s, age = %s WHERE id = %s'
        cursor.execute(query,updated_member)
        conn.commit()
        return jsonify({"Message": "Member details were successfully updated."}), 201

    except Error as e:
        print(f"Error: {e}")
        return jsonify({"Error": "Internal Server Error"}), 500

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/members/<int:id>', methods=['DELETE'])
def delete_member(id):
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error": "Database connection failed."}), 500
        cursor = conn.cursor()

        member_to_remove = (id,)

        cursor.execute('SELECT * FROM members WHERE id = %s', member_to_remove)
        member = cursor.fetchone()

        if not member:
            return jsonify({"Error": "User not found"}), 404
        
        query = 'DELETE FROM members WHERE id = %s'
        cursor.execute(query, member_to_remove)
        conn.commit()
        return jsonify({"Message": "Member has been removed from the database"}), 201
        
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"Error": "Internal Server Error"}), 500

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# Task 3

@app.route('/workoutsessions', methods=['GET'])
def get_workout_sessions():
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error": "Database connection failed."}), 500
        cursor = conn.cursor(dictionary=True)

        query = 'SELECT * FROM workoutsessions'
        cursor.execute(query)

        workout_sessions = cursor.fetchall()
        return sessions_schema.jsonify(workout_sessions)

    except Error as e:
        print(f"Error: {e}")
        return jsonify({"Error": "Internal Server Error"}), 500

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/workoutsessions', methods=['POST'])
def schedule_session():
    try:
        workout_session_data = session_schema.load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error": "Database connection failed."}), 500
        cursor = conn.cursor()

        new_workout_session = (workout_session_data['member_id'], workout_session_data['session_date'], workout_session_data['session_time'], workout_session_data['activity'])

        query = 'INSERT INTO workoutsessions (member_id, session_date, session_time, activity) VALUES(%s,%s,%s,%s)'
        cursor.execute(query,new_workout_session)
        conn.commit()
        return jsonify({"Message": "New workout session has successfully added"}), 201

    except Error as e:
        print(f"Error: {e}")
        return jsonify({"Error": "Internal Server Error"}), 500

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/workoutsessions/<int:member_id>', methods=['PUT'])
def update_workout_session(member_id):
    try:
        workout_session_data = session_schema.load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error": "Database connection failed."}), 500
        cursor = conn.cursor()

        updated_workout_session = (workout_session_data['session_date'], workout_session_data['session_time'], workout_session_data['activity'], member_id)

        query = 'UPDATE workoutsessions SET session_date = %s, session_time = %s, activity = %s WHERE member_id = %s'
        cursor.execute(query,updated_workout_session)
        conn.commit()
        return jsonify({"Message": "Workout session has been successfully updated."}), 201

    except Error as e:
        print(f"Error: {e}")
        return jsonify({"Error": "Internal Server Error"}), 500

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/workoutsessions/<int:member_id>', methods=['GET'])
def display_sessions_for_members(member_id):
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"Error": "Database connection failed."}), 500
        cursor = conn.cursor(dictionary=True)

        member_to_search = (member_id,)

        cursor.execute('SELECT * FROM members WHERE id = %s', member_to_search)
        member = cursor.fetchone()

        if not member:
            return jsonify({"Error": "Member ID does not exist"}), 404
        
        query = 'SELECT * FROM workoutsessions WHERE member_id = %s'
        cursor.execute(query, member_to_search)
        sessions = cursor.fetchall()
        return sessions_schema.jsonify(sessions)
        
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"Error": "Internal Server Error"}), 500

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ =='__main__':
    app.run(debug=True)