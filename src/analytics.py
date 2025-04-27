from datetime import datetime 
import logging
from boto3.dynamodb.conditions import Key
import os
import pandas as pd 
import numpy as np
from decimal import Decimal, InvalidOperation

def query_table(table, chat_id):
    """
    Queries a DynamoDB table for items associated with a specific chat ID.

    Args:
        table (boto3.resources.factory.dynamodb.Table): The DynamoDB table to query.
        chat_id (str): The chat ID to filter the query.

    Returns:
        list: A list of items retrieved from the table, or None if an error occurs.
    """
    try:
        response = table.query(
            KeyConditionExpression=Key('chat_id').eq(chat_id)  # Query by partition key
        )
        items = response.get('Items', [])
        return items
    except Exception as e:
        print(f"Error querying table: {e}")
        return None

def transform_time_columns(workouts):
    """
    Transforms time-related columns in the workouts DataFrame.

    Args:
        workouts (pd.DataFrame): The DataFrame containing workout data.

    Returns:
        pd.DataFrame: The modified DataFrame with transformed date and timestamp columns.
    """
    workouts['workout_date'] = pd.to_datetime(workouts['workout_date'])
    workouts['timestamp']  = workouts['timestamp'].astype(int)
    workouts['timestamp'] = pd.to_datetime(workouts['timestamp'], unit='s')
    return workouts

def unify_weights(workouts):
    """
    Converts weights in the workouts DataFrame to a unified format in kilograms and pounds.

    Args:
        workouts (pd.DataFrame): The DataFrame containing workout data with weight information.

    Returns:
        pd.DataFrame: The modified DataFrame with unified weight columns in kg and lbs.
    """
    workouts['weight'] = workouts['weight'].astype(float)
    workouts['weight_kg'] = np.where(workouts['weight_unit'] == 'kg', workouts['weight'], workouts['weight'] * 0.45359237).round(2)
    workouts['weight_lbs'] = np.where(workouts['weight_unit'] == 'lbs', workouts['weight'], workouts['weight'] / 0.45359237).round(2)
    return workouts

def get_max_workout_weights(workouts):
    """
    Retrieves the maximum weights lifted for each exercise type from the workouts DataFrame.

    Args:
        workouts (pd.DataFrame): The DataFrame containing workout data.

    Returns:
        pd.DataFrame: A DataFrame containing the maximum weights for each exercise type, excluding cardio.
    """
    reps_order = {'5x12':1,'5x7':2,'7x5':3,'10x3':4,'15-12-10-9-8':5}
    workouts['reps_order'] = workouts['sets_reps'].map(reps_order)
    max_weights = workouts.groupby(['workout_type','exercise','reps_order','sets_reps']).agg({'weight_kg': 'max', 'weight_lbs':'max'}).reset_index()

    max_weights = max_weights.sort_values(by=['workout_type','exercise', 'weight_kg', 'reps_order'], ascending=[True,True, False, True])
    max_weights_filtered = max_weights.groupby('exercise', as_index=False).first()
    max_weights_filtered = max_weights_filtered[max_weights_filtered['workout_type'] != 'Cardio']
    max_weights_filtered = max_weights_filtered.sort_values(by=['workout_type', 'exercise', 'weight_kg', 'reps_order'], ascending=[True, True, False, True])
    max_weights_filtered = max_weights_filtered[['workout_type', 'exercise', 'sets_reps', 'weight_kg', 'weight_lbs']]
    return max_weights_filtered

def get_latest_workout_type(workouts):
    """
    Retrieves the latest workout type based on the most recent timestamp for each workout type.

    Args:
        workouts (pd.DataFrame): The DataFrame containing workout data.

    Returns:
        pd.DataFrame: A DataFrame containing the latest timestamp for each workout type.
    """
    workout_groupby = workouts.groupby('workout_type').agg({'timestamp': 'max'}).reset_index()
    return workout_groupby

def generate_analytics_message():
    """
    Generates a message string with analytics options for the user.

    Returns:
        str: A formatted message string with available analytics commands.
    """
    analytics_message = (
    "Type /max_weights_pull for your maximum pull weights.\n"
    "Type /max_weights_push for your maximum push weights.\n"
    "Type /max_weights_legs for your maximum leg weights.\n"
    "Type /last_workout to get the latest training dates.\n"
)
    return analytics_message



def format_workout_message(df):
    """
    Formats a DataFrame into a message string for the Telegram bot.

    Args:
        df (pd.DataFrame): The DataFrame containing workout details.

    Returns:
        str: A formatted message string.
    """
    # Initialize the message
    message = "🏋️‍♂️ *Your Maximum Weights* 🏋️‍♀️\n\n"

    # Iterate through the DataFrame rows
    for _, row in df.iterrows():
        # Format each row into a string
        message += (
            f"Exercise: {row['exercise']}\n"
            f"🔸 Workout Type: {row['workout_type']}\n"
            f"🔸 Sets/Reps: {row['sets_reps']}\n"
            f"🔸 Weight: {row['weight_kg']} kg / {row['weight_lbs']} lbs\n\n"
        )

    return message

def generate_last_workout_message(df):
    """
    Generates a message summarizing the last time each workout type was trained.

    Args:
        df (pd.DataFrame): A DataFrame with columns 'workout_type' and 'timestamp'.

    Returns:
        str: A formatted message summarizing the last workout times.
    """
    # Ensure the timestamp column is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Sort the DataFrame by timestamp in descending order
    df = df.sort_values(by='timestamp', ascending=False)

    # Generate the message
    message = "🏋️‍♂️ *Last Workout Summary* 🏋️‍♀️\n\n"
    for _, row in df.iterrows():
        workout_type = row['workout_type']
        last_trained = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        message += f"🔹 *{workout_type}*: Last trained on {last_trained}\n"

    return message